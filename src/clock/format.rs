pub fn format_offset_smart(micros: i64) -> String {
    let sign = if micros > 0 {
        "+"
    } else if micros < 0 {
        "-"
    } else {
        ""
    };

    let abs_us = micros.unsigned_abs();

    if abs_us == 0 {
        "0µs".to_string()
    } else if abs_us < 1_000 {
        format!("{sign}{abs_us}µs")
    } else if abs_us < 1_000_000 {
        let ms = abs_us as f64 / 1_000.0;
        format!("{sign}{ms:.1}ms")
    } else if abs_us < 60_000_000 {
        let sec = abs_us as f64 / 1_000_000.0;
        format!("{sign}{sec:.2}s")
    } else {
        let total_sec = abs_us / 1_000_000;
        let mins = total_sec / 60;
        let rem_sec = total_sec % 60;
        format!("{sign}{mins}m {rem_sec:02}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_offset_zero() {
        assert_eq!(format_offset_smart(0), "0µs");
    }

    #[test]
    fn test_format_offset_sub_millisecond() {
        assert_eq!(format_offset_smart(42), "+42µs");
        assert_eq!(format_offset_smart(-18), "-18µs");
        assert_eq!(format_offset_smart(999), "+999µs");
        assert_eq!(format_offset_smart(-999), "-999µs");
    }

    #[test]
    fn test_format_offset_milliseconds() {
        assert_eq!(format_offset_smart(1_000), "+1.0ms");
        assert_eq!(format_offset_smart(12_400), "+12.4ms");
        assert_eq!(format_offset_smart(-350_200), "-350.2ms");
        assert_eq!(format_offset_smart(999_900), "+999.9ms");
    }

    #[test]
    fn test_format_offset_seconds() {
        assert_eq!(format_offset_smart(1_000_000), "+1.00s");
        assert_eq!(format_offset_smart(2_450_000), "+2.45s");
        assert_eq!(format_offset_smart(-8_953_096), "-8.95s");
        assert_eq!(format_offset_smart(59_900_000), "+59.90s");
    }

    #[test]
    fn test_format_offset_minutes() {
        assert_eq!(format_offset_smart(60_000_000), "+1m 00s");
        assert_eq!(format_offset_smart(72_000_000), "+1m 12s");
        assert_eq!(format_offset_smart(720_683_184), "+12m 00s");
        assert_eq!(format_offset_smart(-398_120_642), "-6m 38s");
    }
}
