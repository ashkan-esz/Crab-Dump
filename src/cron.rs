//! Minimal 5-field crontab parser and "when does this fire next" evaluator.
//!
//! Supports the standard vixie-cron field syntax operators used in a crontab
//! line, in `minute hour day-of-month month day-of-week` order:
//!
//! ```text
//! *            every value
//! */4          every 4th value from the start of the range (0, 4, 8, …)
//! 5            one value
//! 9-17         an inclusive range
//! 9-17/2       a range with a step (9, 11, 13, 15, 17)
//! 1,15,30      a list (each item may itself be any of the forms above)
//! jan..dec     month names (3 letters, case-insensitive)
//! sun..sat     day-of-week names; `7` is also Sunday
//! ```
//!
//! Times are interpreted in the machine's **local** clock time, like crontab.
//! Matching works on the local wall clock, so around a DST transition a single
//! cycle can land an hour early or late, and a time inside a spring-forward gap
//! does not fire that day. A backup scheduler does not need better than that;
//! set `TZ=UTC` if you want the schedule immune to it.
//!
//! Not supported (`BACKUP_INTERVAL` covers the same ground, or crontab-only
//! features with no meaning here): `@daily`-style nicknames, `L`/`W`/`#`
//! extensions, and seconds as a sixth field.

use anyhow::{bail, Context, Result};
use chrono::{Datelike, Duration, NaiveDateTime, Timelike};

/// Month names, indexed from January. Matched case-insensitively.
const MONTH_NAMES: [&str; 12] = [
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
];

/// Day-of-week names, indexed from Sunday (cron's day 0).
const DOW_NAMES: [&str; 7] = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];

/// Upper bound on the minute/day steps [`Cron::next_after`] will take before
/// giving up.
///
/// The search skips whole days that cannot match, so a real expression is found
/// within roughly `1440 + 4 × 366 + 1440` steps — four years covers the worst
/// realistic case, a leap-day expression. Exhausting the budget means the
/// expression can never fire (`0 0 30 2 *` — February 30th), which
/// [`Cron::parse`] rejects up front.
const MAX_SEARCH_STEPS: u32 = 8_000;

/// A parsed crontab expression.
///
/// Each field is a bitmask over its legal values, so matching a candidate time
/// is five bit tests instead of any re-parsing.
#[derive(Debug, Clone)]
pub struct Cron {
    /// The expression as written, kept for log lines and error messages.
    expr: String,
    /// Bit *n* set → fires at minute *n* (0–59).
    minute: u64,
    /// Bit *n* set → fires at hour *n* (0–23).
    hour: u64,
    /// Bit *n* set → fires on day-of-month *n* (1–31).
    dom: u64,
    /// Bit *n* set → fires in month *n* (1–12).
    month: u64,
    /// Bit *n* set → fires on weekday *n* (0–6, Sunday = 0).
    dow: u64,
    /// Whether the day-of-month field was something other than `*`.
    /// Together with `dow_restricted` this drives cron's day-matching rule
    /// (see [`Cron::date_matches`]).
    dom_restricted: bool,
    /// Whether the day-of-week field was something other than `*`.
    dow_restricted: bool,
}

impl Cron {
    /// Parse a 5-field crontab expression.
    ///
    /// Fails on the wrong field count, out-of-range values, malformed steps or
    /// ranges, and on expressions that can never fire (e.g. `0 0 30 2 *`) —
    /// all of which are better caught at startup than by a scheduler that
    /// silently never runs.
    pub fn parse(expr: &str) -> Result<Self> {
        let fields: Vec<&str> = expr.split_whitespace().collect();
        if fields.len() != 5 {
            bail!(
                "cron expression needs exactly 5 fields \
                 (minute hour day-of-month month day-of-week), got {} in `{expr}`",
                fields.len(),
            );
        }

        let minute = parse_field(fields[0], 0, 59, &[]).context("cron minute field")?;
        let hour = parse_field(fields[1], 0, 23, &[]).context("cron hour field")?;
        let dom = parse_field(fields[2], 1, 31, &[]).context("cron day-of-month field")?;
        let month = parse_field(fields[3], 1, 12, &MONTH_NAMES).context("cron month field")?;
        // Day 7 is another spelling of Sunday; fold it onto day 0 so matching
        // only ever has to look at bits 0–6.
        let mut dow = parse_field(fields[4], 0, 7, &DOW_NAMES).context("cron day-of-week field")?;
        if dow & (1 << 7) != 0 {
            dow = (dow & !(1 << 7)) | 1;
        }

        let cron = Self {
            expr: expr.trim().to_string(),
            minute,
            hour,
            dom,
            month,
            dow,
            // `*` (and only `*`) leaves a day field unrestricted. `*/1` is
            // spelled differently but means the same, so treat a full mask as
            // unrestricted too — otherwise `* * */1 * mon` would OR the two
            // day fields and fire every day.
            dom_restricted: dom != full_mask(1, 31),
            dow_restricted: dow & 0b111_1111 != 0b111_1111,
        };

        // An expression that matches no real date parses fine but would leave
        // the scheduler asleep forever — reject it while the operator is still
        // looking at the terminal.
        let now = chrono::Local::now().naive_local();
        if cron.next_after(now).is_none() {
            bail!(
                "cron expression `{expr}` never fires (no matching date within \
                 the next four years) — check the day-of-month/month combination",
            );
        }

        Ok(cron)
    }

    /// The first firing time strictly after `from`, in local clock time.
    ///
    /// Returns `None` only for an expression that cannot fire — see
    /// [`MAX_SEARCH_STEPS`].
    pub fn next_after(&self, from: NaiveDateTime) -> Option<NaiveDateTime> {
        // Start at the next whole minute: cron has minute resolution, and
        // `from` itself must not match (the caller has just run that minute).
        let mut t = from
            .with_second(0)?
            .with_nanosecond(0)?
            .checked_add_signed(Duration::minutes(1))?;

        for _ in 0..MAX_SEARCH_STEPS {
            // Nothing on a non-matching date can fire, so skip the whole day
            // instead of walking its 1440 minutes.
            if !self.date_matches(t) {
                t = t.date().succ_opt()?.and_hms_opt(0, 0, 0)?;
                continue;
            }
            if self.minute & bit(t.minute()) != 0 && self.hour & bit(t.hour()) != 0 {
                return Some(t);
            }
            // Rolling past midnight re-enters the date check above.
            t = t.checked_add_signed(Duration::minutes(1))?;
        }
        None
    }

    /// Whether `t`'s date can fire, ignoring the time of day.
    ///
    /// Day-of-month and day-of-week are **OR**ed when both are restricted —
    /// cron's long-standing quirk, which makes `0 0 1 * mon` mean "the 1st, and
    /// every Monday", not "Mondays that fall on the 1st". When only one of them
    /// is restricted the other is a full mask, so the AND below is a no-op and
    /// the single restriction decides.
    fn date_matches(&self, t: NaiveDateTime) -> bool {
        if self.month & bit(t.month()) == 0 {
            return false;
        }
        let dom_hit = self.dom & bit(t.day()) != 0;
        let dow_hit = self.dow & bit(t.weekday().num_days_from_sunday()) != 0;
        if self.dom_restricted && self.dow_restricted {
            dom_hit || dow_hit
        } else {
            dom_hit && dow_hit
        }
    }
}

impl std::fmt::Display for Cron {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.expr)
    }
}

/// Bit `n`, for values that always fit in the 0–59 masks used here.
fn bit(n: u32) -> u64 {
    1u64 << n
}

/// Mask with every bit in `min..=max` set.
fn full_mask(min: u32, max: u32) -> u64 {
    (min..=max).fold(0, |m, v| m | bit(v))
}

/// Parse one comma-separated cron field into a bitmask over `min..=max`.
///
/// `names` supplies the optional 3-letter aliases for the field (month or
/// day-of-week), indexed so that `names[0]` is the value `min`.
fn parse_field(spec: &str, min: u32, max: u32, names: &[&str]) -> Result<u64> {
    let spec = spec.trim();
    if spec.is_empty() {
        bail!("empty field");
    }

    let mut mask = 0u64;
    for item in spec.split(',') {
        let item = item.trim();

        // Split off an optional `/step`. Anything else with a slash in it is a
        // typo we should report rather than half-parse.
        let (range_part, step) = match item.split_once('/') {
            Some((r, s)) => {
                let step: u32 = s
                    .trim()
                    .parse()
                    .map_err(|_| anyhow::anyhow!("`{item}`: step must be a positive number"))?;
                if step == 0 {
                    bail!("`{item}`: step must be at least 1");
                }
                (r.trim(), step)
            }
            None => (item, 1),
        };

        // Resolve the range the step walks over.
        let (start, end) = if range_part == "*" {
            (min, max)
        } else if let Some((lo, hi)) = range_part.split_once('-') {
            let lo = parse_value(lo, min, max, names)?;
            let hi = parse_value(hi, min, max, names)?;
            if lo > hi {
                bail!("`{item}`: range start {lo} is after its end {hi}");
            }
            (lo, hi)
        } else {
            let v = parse_value(range_part, min, max, names)?;
            // A bare value with a step means "from here to the end of the
            // range", the vixie-cron reading of `5/10`. Without a step it is
            // just the single value.
            if step > 1 {
                (v, max)
            } else {
                (v, v)
            }
        };

        for v in (start..=end).step_by(step as usize) {
            mask |= bit(v);
        }
    }

    Ok(mask)
}

/// Parse a single cron value: a number in `min..=max`, or a 3-letter name.
fn parse_value(raw: &str, min: u32, max: u32, names: &[&str]) -> Result<u32> {
    let raw = raw.trim();
    if raw.is_empty() {
        bail!("missing value");
    }

    // Names are only legal in the month and day-of-week fields, which are the
    // only ones that pass a non-empty `names`.
    let lower = raw.to_ascii_lowercase();
    if let Some(idx) = names.iter().position(|n| *n == lower) {
        return Ok(min + idx as u32);
    }

    let value: u32 = raw.parse().map_err(|_| {
        if names.is_empty() {
            anyhow::anyhow!("`{raw}` is not a number in {min}–{max}")
        } else {
            anyhow::anyhow!(
                "`{raw}` is not a number in {min}–{max} or one of {}",
                names.join("/"),
            )
        }
    })?;

    if value < min || value > max {
        bail!("{value} is out of range {min}–{max}");
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn at(y: i32, m: u32, d: u32, h: u32, mi: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(h, mi, 0)
            .unwrap()
    }

    /// Collect the next `n` firing times, walking forward from each result the
    /// way the scheduler does.
    fn next_n(expr: &str, from: NaiveDateTime, n: usize) -> Vec<NaiveDateTime> {
        let cron = Cron::parse(expr).expect("expression must parse");
        let mut out = Vec::new();
        let mut t = from;
        for _ in 0..n {
            t = cron.next_after(t).expect("expression must fire");
            out.push(t);
        }
        out
    }

    /// The case from the request: `*/4` in the hour field is midnight, 04:00,
    /// 08:00 … — aligned to the wall clock, not to when the process started.
    #[test]
    fn step_in_hour_field_fires_every_fourth_hour() {
        let got = next_n("0 */4 * * *", at(2026, 8, 12, 9, 30), 5);
        assert_eq!(
            got,
            vec![
                at(2026, 8, 12, 12, 0),
                at(2026, 8, 12, 16, 0),
                at(2026, 8, 12, 20, 0),
                at(2026, 8, 13, 0, 0),
                at(2026, 8, 13, 4, 0),
            ],
        );
    }

    #[test]
    fn single_daily_time_fires_once_a_day() {
        let got = next_n("30 3 * * *", at(2026, 8, 12, 3, 30), 2);
        assert_eq!(got, vec![at(2026, 8, 13, 3, 30), at(2026, 8, 14, 3, 30)]);
    }

    /// A time exactly on a firing minute must not re-fire that same minute,
    /// otherwise a fast cycle would loop on it.
    #[test]
    fn next_is_strictly_after_the_given_time() {
        let cron = Cron::parse("*/15 * * * *").unwrap();
        let t = at(2026, 8, 12, 9, 15);
        assert_eq!(cron.next_after(t), Some(at(2026, 8, 12, 9, 30)));
        // Mid-minute input still advances to the next whole firing minute.
        assert_eq!(
            cron.next_after(t.with_second(42).unwrap()),
            Some(at(2026, 8, 12, 9, 30)),
        );
    }

    #[test]
    fn lists_ranges_and_range_steps_combine() {
        // 09:00, 09:30, 12:00, 12:30 — list in the hour field, list in minutes.
        let got = next_n("0,30 9,12 * * *", at(2026, 8, 12, 8, 0), 4);
        assert_eq!(
            got,
            vec![
                at(2026, 8, 12, 9, 0),
                at(2026, 8, 12, 9, 30),
                at(2026, 8, 12, 12, 0),
                at(2026, 8, 12, 12, 30),
            ],
        );

        // A stepped range: 9, 11, 13 … and nothing at 10.
        let cron = Cron::parse("0 9-13/2 * * *").unwrap();
        for (hour, fires) in [(9, true), (10, false), (11, true), (13, true), (15, false)] {
            let hit = cron.next_after(at(2026, 8, 12, hour, 0) - Duration::minutes(1))
                == Some(at(2026, 8, 12, hour, 0));
            assert_eq!(hit, fires, "hour {hour}");
        }
    }

    /// Weekday names and ranges of them: the standard "business hours" line.
    #[test]
    fn weekday_names_and_ranges_are_honoured() {
        // 2026-08-14 is a Friday; 15th/16th are the weekend, 17th a Monday.
        let got = next_n("0 9 * * mon-fri", at(2026, 8, 14, 9, 0), 2);
        assert_eq!(got, vec![at(2026, 8, 17, 9, 0), at(2026, 8, 18, 9, 0)]);

        // Sunday accepts both spellings, 0 and 7.
        for expr in ["0 5 * * 0", "0 5 * * 7", "0 5 * * sun"] {
            assert_eq!(
                Cron::parse(expr).unwrap().next_after(at(2026, 8, 12, 0, 0)),
                Some(at(2026, 8, 16, 5, 0)),
                "{expr} must resolve to the coming Sunday",
            );
        }
    }

    #[test]
    fn month_names_restrict_to_that_month() {
        let got = next_n("0 0 1 jan *", at(2026, 8, 12, 0, 0), 2);
        assert_eq!(got, vec![at(2027, 1, 1, 0, 0), at(2028, 1, 1, 0, 0)]);
    }

    /// Cron ORs day-of-month with day-of-week when both are restricted. Getting
    /// this wrong silently skips backups, so it is pinned here.
    #[test]
    fn day_of_month_and_weekday_are_ored_when_both_restricted() {
        // The 1st *or* any Monday. 2026-08-12 is a Wednesday; the 17th is the
        // next Monday and the 1st of September comes after it.
        let got = next_n("0 0 1 * mon", at(2026, 8, 12, 0, 0), 3);
        assert_eq!(
            got,
            vec![
                at(2026, 8, 17, 0, 0),
                at(2026, 8, 24, 0, 0),
                at(2026, 8, 31, 0, 0),
            ],
        );

        // Only one field restricted → plain AND semantics, so a day-of-month
        // expression does not fire on other days.
        let got = next_n("0 0 15 * *", at(2026, 8, 12, 0, 0), 2);
        assert_eq!(got, vec![at(2026, 8, 15, 0, 0), at(2026, 9, 15, 0, 0)]);
    }

    /// `*/1` in a day field means "every day", so it must not turn on the OR
    /// rule and quietly widen an adjacent weekday restriction.
    #[test]
    fn full_day_field_is_not_treated_as_restricted() {
        let got = next_n("0 0 */1 * mon", at(2026, 8, 12, 0, 0), 2);
        assert_eq!(got, vec![at(2026, 8, 17, 0, 0), at(2026, 8, 24, 0, 0)]);
    }

    #[test]
    fn every_minute_fires_every_minute() {
        let got = next_n("* * * * *", at(2026, 8, 12, 23, 58), 3);
        assert_eq!(
            got,
            vec![
                at(2026, 8, 12, 23, 59),
                at(2026, 8, 13, 0, 0),
                at(2026, 8, 13, 0, 1),
            ],
        );
    }

    /// Leap-day expressions are the reason the search budget spans four years.
    #[test]
    fn leap_day_expression_finds_the_next_leap_year() {
        let got = next_n("0 3 29 feb *", at(2026, 8, 12, 0, 0), 1);
        assert_eq!(got, vec![at(2028, 2, 29, 3, 0)]);
    }

    #[test]
    fn malformed_expressions_are_rejected_with_a_reason() {
        for (expr, needle) in [
            ("0 */4 * *", "exactly 5 fields"),
            ("0 */4 * * * *", "exactly 5 fields"),
            ("60 * * * *", "out of range"),
            ("0 24 * * *", "out of range"),
            ("0 0 0 * *", "out of range"),
            ("0 0 * 13 *", "out of range"),
            ("0 0 * * 8", "out of range"),
            ("*/0 * * * *", "at least 1"),
            ("*/x * * * *", "step must be"),
            ("17-9 * * * *", "after its end"),
            ("0 0 * xyz *", "jan/feb"),
            ("nope * * * *", "not a number"),
            ("0 0 30 2 *", "never fires"),
        ] {
            let err = Cron::parse(expr).unwrap_err();
            let msg = format!("{err:#}");
            assert!(
                msg.contains(needle),
                "`{expr}`: expected an error mentioning `{needle}`, got: {msg}",
            );
        }
    }

    /// The field a bad value came from must be named — "out of range 0–23" on
    /// its own does not say which of five fields to fix.
    #[test]
    fn errors_name_the_offending_field() {
        let msg = format!("{:#}", Cron::parse("0 99 * * *").unwrap_err());
        assert!(msg.contains("hour"), "unexpected error: {msg}");
    }
}
