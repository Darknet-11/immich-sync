pub enum DeleteAgeEligibility {
    Eligible(i64),
    MissingCreatedAt,
    FutureCreatedAt(i64),
    UnrealisticAge(i64),
}

pub fn evaluate_delete_age(age_days: Option<i64>, delete_max_age: i64) -> DeleteAgeEligibility {
    match age_days {
        Some(age) if (0..=delete_max_age).contains(&age) => DeleteAgeEligibility::Eligible(age),
        Some(age) if age < 0 => DeleteAgeEligibility::FutureCreatedAt(age),
        Some(age) => DeleteAgeEligibility::UnrealisticAge(age),
        None => DeleteAgeEligibility::MissingCreatedAt,
    }
}

pub fn should_propagate_local_delete(age_days: i64, delete_threshold: i64) -> bool {
    age_days < delete_threshold
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evaluate_delete_age_missing() {
        assert!(matches!(evaluate_delete_age(None, 3650), DeleteAgeEligibility::MissingCreatedAt));
    }

    #[test]
    fn evaluate_delete_age_future() {
        assert!(matches!(evaluate_delete_age(Some(-1), 3650), DeleteAgeEligibility::FutureCreatedAt(-1)));
    }

    #[test]
    fn evaluate_delete_age_unrealistic() {
        assert!(matches!(evaluate_delete_age(Some(3651), 3650), DeleteAgeEligibility::UnrealisticAge(3651)));
    }

    #[test]
    fn evaluate_delete_age_boundary_values() {
        assert!(matches!(evaluate_delete_age(Some(0), 3650), DeleteAgeEligibility::Eligible(0)));
        assert!(matches!(evaluate_delete_age(Some(3650), 3650), DeleteAgeEligibility::Eligible(3650)));
    }

    #[test]
    fn propagate_local_delete_threshold_behavior() {
        assert!(should_propagate_local_delete(0, 1));
        assert!(!should_propagate_local_delete(0, 0));
        assert!(!should_propagate_local_delete(365, 365));
        assert!(!should_propagate_local_delete(366, 365));
    }
}
