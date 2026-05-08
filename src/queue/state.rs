use std::sync::RwLock;

use crate::error::{Result, SynkroError};
use crate::protocol::messages::{QueueCommand, QueueState, Track};

pub struct QueueManager {
    state: RwLock<QueueState>,
}

impl QueueManager {
    pub fn new(initial_state: QueueState) -> Self {
        Self {
            state: RwLock::new(initial_state),
        }
    }

    pub fn apply(&self, cmd: QueueCommand) -> Result<QueueState> {
        let mut state = self.state.write().unwrap();
        let changed = match cmd {
            QueueCommand::Add(track) => {
                if state.current.is_none() {
                    state.current = Some(track);
                } else {
                    state.upcoming.push(track);
                }
                true
            }
            QueueCommand::Skip => {
                Self::advance_locked(&mut state);
                true
            }
            QueueCommand::Remove { position } => {
                if position < state.upcoming.len() {
                    state.upcoming.remove(position);
                    true
                } else {
                    false
                }
            }
        };

        if changed {
            state.version += 1;
        }

        Ok(state.clone())
    }

    pub fn accept_update(&self, incoming: QueueState) -> Result<()> {
        let mut state = self.state.write().unwrap();
        if incoming.version <= state.version {
            return Err(SynkroError::StaleQueue {
                local: state.version,
                received: incoming.version,
            });
        }

        *state = incoming;
        Ok(())
    }

    pub fn force_set(&self, incoming: QueueState) {
        *self.state.write().unwrap() = incoming;
    }

    pub fn advance(&self) -> Option<Track> {
        let mut state = self.state.write().unwrap();
        let next = Self::advance_locked(&mut state);
        state.version += 1;
        next
    }

    pub fn snapshot(&self) -> QueueState {
        self.state.read().unwrap().clone()
    }

    fn advance_locked(state: &mut QueueState) -> Option<Track> {
        state.current = if state.upcoming.is_empty() {
            None
        } else {
            Some(state.upcoming.remove(0))
        };
        state.current.clone()
    }
}
