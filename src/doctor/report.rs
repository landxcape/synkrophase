use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CheckStatus {
    Passed,
    Warning,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticCheck {
    pub name: String,
    pub category: String,
    pub status: CheckStatus,
    pub message: String,
    pub details: Option<String>,
    pub remediation: Option<String>,
    pub is_fixable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoctorReport {
    pub version: String,
    pub all_passed: bool,
    pub checks: Vec<DiagnosticCheck>,
    pub fixable_count: usize,
    pub repairs_applied: Vec<String>,
}

impl DoctorReport {
    pub fn new(version: String) -> Self {
        Self {
            version,
            all_passed: true,
            checks: Vec::new(),
            fixable_count: 0,
            repairs_applied: Vec::new(),
        }
    }

    pub fn add_check(&mut self, check: DiagnosticCheck) {
        if check.status == CheckStatus::Failed {
            self.all_passed = false;
        }
        if check.is_fixable {
            self.fixable_count += 1;
        }
        self.checks.push(check);
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    pub fn print_terminal(&self) {
        println!("=== Synkrophase Doctor (v{}) ===", self.version);
        println!();

        if !self.repairs_applied.is_empty() {
            println!("🛠  Repairs Applied:");
            for repair in &self.repairs_applied {
                println!("  ✓ {repair}");
            }
            println!();
        }

        let mut current_category = "";
        for check in &self.checks {
            if check.category != current_category {
                current_category = &check.category;
                println!("[{}]", current_category);
            }

            let symbol = match check.status {
                CheckStatus::Passed => "  \x1b[32m✓\x1b[0m",
                CheckStatus::Warning => "  \x1b[33m!\x1b[0m",
                CheckStatus::Failed => "  \x1b[31m✗\x1b[0m",
            };

            print!("{} {}: {}", symbol, check.name, check.message);
            if let Some(details) = &check.details {
                print!(" ({details})");
            }
            println!();

            if let Some(remediation) = &check.remediation {
                println!("    \x1b[36m↳ Action\x1b[0m: {remediation}");
            }
        }

        println!();
        if self.all_passed && self.fixable_count == 0 {
            println!(
                "\x1b[32m=== Diagnostic Result: All systems operational. Ready to sync. ===\x1b[0m"
            );
        } else if self.fixable_count > 0 {
            println!(
                "\x1b[33m=== Diagnostic Result: Found {} issue(s) that can be automatically repaired. ===\x1b[0m",
                self.fixable_count
            );
            println!("Run 'synkro doctor --fix' to automatically apply repairs.");
        } else {
            println!(
                "\x1b[31m=== Diagnostic Result: Configuration or environment issues detected. ===\x1b[0m"
            );
            println!("Review the remediation instructions above.");
        }
    }
}
