//! Alarm lifecycle and notification scheduling in source-ordered paused time.
//!
//! This module is not a connector and never reads a clock. The caller drains
//! due work before each input and commits the complete state with that cut.
//! Lifecycle transitions are never suppressed by notification cooldown.

use sparrow_model::{ErrorCode, Result, SparrowError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Policy {
    pub activate_micros: i64,
    pub resolve_micros: i64,
    pub cooldown_micros: i64,
    pub notification_max_age_micros: i64,
}

fn invalid(message: &str) -> SparrowError {
    SparrowError::new(ErrorCode::CodecViolation, message)
}
fn add(at: i64, duration: i64) -> Result<i64> {
    at.checked_add(duration)
        .ok_or_else(|| invalid("alarm deadline overflow"))
}

impl Policy {
    pub fn validate(self) -> Result<()> {
        if self.activate_micros < 0
            || self.resolve_micros < 0
            || self.cooldown_micros < 0
            || self.notification_max_age_micros <= 0
        {
            return Err(invalid(
                "alarm durations must be nonnegative and notification max age positive",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    Normal,
    Pending,
    Active,
    Recovering,
}

impl Phase {
    pub fn tag(self) -> u64 {
        match self {
            Self::Normal => 0,
            Self::Pending => 1,
            Self::Active => 2,
            Self::Recovering => 3,
        }
    }
    pub fn from_tag(tag: u64) -> Result<Self> {
        match tag {
            0 => Ok(Self::Normal),
            1 => Ok(Self::Pending),
            2 => Ok(Self::Active),
            3 => Ok(Self::Recovering),
            _ => Err(invalid("unknown alarm phase")),
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Pending => "pending",
            Self::Active => "active",
            Self::Recovering => "recovering",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EventKind {
    Activate,
    Resolve,
    Notify,
}

impl EventKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Activate => "activate",
            Self::Resolve => "resolve",
            Self::Notify => "notify",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Event {
    pub kind: EventKind,
    pub phase: Phase,
    pub episode: u64,
    pub notify: bool,
    /// Actual durable decision time, not an invented intervening observation.
    pub at: i64,
}

/// Scalar-only metadata; row/key ownership lives in the bounded operator.
/// Normal entries retain their counter, so keys cannot be silently evicted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct State {
    pub phase: Phase,
    pub since: i64,
    pub deadline: Option<i64>,
    pub episode: u64,
    pub not_before: i64,
    pub notification_since: Option<i64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Effects {
    pub event: Option<Event>,
    pub notification_expired: bool,
    pub notification_cancelled: bool,
}

impl State {
    pub fn new(now: i64) -> Result<Self> {
        if now < 0 {
            return Err(invalid("negative alarm time"));
        }
        Ok(Self {
            phase: Phase::Normal,
            since: now,
            deadline: None,
            episode: 0,
            not_before: 0,
            notification_since: None,
        })
    }

    fn notification_deadline(self, policy: Policy) -> Result<Option<i64>> {
        self.notification_since
            .map(|since| {
                Ok(self
                    .not_before
                    .min(add(since, policy.notification_max_age_micros)?))
            })
            .transpose()
    }

    pub fn next_deadline(self, policy: Policy) -> Result<Option<i64>> {
        Ok(self
            .deadline
            .into_iter()
            .chain(self.notification_deadline(policy)?)
            .min())
    }

    /// Check a fully drained checkpoint state, including the last valid input.
    pub fn validate(self, policy: Policy, now: i64, enter: bool, clear: bool) -> Result<()> {
        self.validate_conditions(policy, enter, clear)?;
        if now < 0
            || self.since < 0
            || self.since > now
            || self.not_before < 0
            || (enter && clear)
            || self.not_before > now.saturating_add(policy.cooldown_micros)
            || (self.episode == 0 && (self.not_before != 0 || self.notification_since.is_some()))
        {
            return Err(invalid("alarm metadata/condition mismatch"));
        }
        if self.next_deadline(policy)?.is_some_and(|at| at <= now)
            || self.notification_since.is_some_and(|at| at > now)
        {
            return Err(invalid(
                "alarm cut has an overdue timer or future notification",
            ));
        }
        Ok(())
    }

    /// Policy/schema dependent checks also run during atomic restore, before
    /// adopting any decoded entry. Cut-time checks follow before activation.
    pub fn validate_conditions(self, policy: Policy, enter: bool, clear: bool) -> Result<()> {
        policy.validate()?;
        if (enter && clear)
            || self.since < 0
            || self.not_before < 0
            || (self.episode == 0 && (self.not_before != 0 || self.notification_since.is_some()))
        {
            return Err(invalid("alarm state/conditions mismatch"));
        }
        let expected = match self.phase {
            Phase::Pending if enter && !clear && policy.activate_micros > 0 => {
                Some(add(self.since, policy.activate_micros)?)
            }
            Phase::Recovering
                if clear && !enter && self.episode > 0 && policy.resolve_micros > 0 =>
            {
                Some(add(self.since, policy.resolve_micros)?)
            }
            Phase::Normal if !enter => None,
            Phase::Active if !clear && self.episode > 0 => None,
            _ => return Err(invalid("alarm phase disagrees with retained conditions")),
        };
        if self.deadline != expected {
            return Err(invalid("alarm state has an inconsistent timer"));
        }
        if let Some(since) = self.notification_since {
            if !matches!(self.phase, Phase::Active | Phase::Recovering)
                || self.episode == 0
                || since < 0
                || self.not_before <= since
            {
                return Err(invalid("alarm pending notification metadata"));
            }
        }
        Ok(())
    }

    fn event(self, kind: EventKind, notify: bool, now: i64) -> Event {
        Event {
            kind,
            phase: self.phase,
            episode: self.episode,
            notify,
            at: now,
        }
    }

    fn activate(&mut self, policy: Policy, now: i64) -> Result<Effects> {
        self.episode = self
            .episode
            .checked_add(1)
            .ok_or_else(|| invalid("alarm episode exhausted"))?;
        self.phase = Phase::Active;
        self.since = now;
        self.deadline = None;
        let notify = now >= self.not_before;
        if notify {
            self.not_before = add(now, policy.cooldown_micros)?;
            self.notification_since = None;
        } else {
            // Validate age overflow before publishing a transition.
            add(now, policy.notification_max_age_micros)?;
            self.notification_since = Some(now);
        }
        Ok(Effects {
            event: Some(self.event(EventKind::Activate, notify, now)),
            ..Effects::default()
        })
    }

    fn resolve(&mut self, policy: Policy, now: i64) -> Result<Effects> {
        let cancelled = self.notification_since.take().is_some();
        self.phase = Phase::Normal;
        self.since = now;
        self.deadline = None;
        // Recovery notifications bypass cooldown, then begin a new cooldown.
        self.not_before = add(now, policy.cooldown_micros)?;
        Ok(Effects {
            event: Some(self.event(EventKind::Resolve, true, now)),
            notification_cancelled: cancelled,
            ..Effects::default()
        })
    }

    /// Returns a candidate. Failure never partially mutates the committed state.
    pub fn observe(
        self,
        policy: Policy,
        now: i64,
        enter: bool,
        clear: bool,
    ) -> Result<(Self, Effects)> {
        policy.validate()?;
        if now < 0 || now < self.since || (enter && clear) {
            return Err(invalid(
                "alarm requires monotonic time and exclusive enter/clear",
            ));
        }
        if self.next_deadline(policy)?.is_some_and(|at| at <= now) {
            return Err(invalid("alarm due work must precede input"));
        }
        let mut next = self;
        let effects = match (self.phase, enter, clear) {
            (Phase::Normal, true, false) => {
                if policy.activate_micros == 0 {
                    next.activate(policy, now)?
                } else {
                    next.phase = Phase::Pending;
                    next.since = now;
                    next.deadline = Some(add(now, policy.activate_micros)?);
                    Effects::default()
                }
            }
            (Phase::Pending, false, _) => {
                next.phase = Phase::Normal;
                next.since = now;
                next.deadline = None;
                Effects::default()
            }
            (Phase::Active, false, true) => {
                if policy.resolve_micros == 0 {
                    next.resolve(policy, now)?
                } else {
                    next.phase = Phase::Recovering;
                    next.since = now;
                    next.deadline = Some(add(now, policy.resolve_micros)?);
                    Effects::default()
                }
            }
            (Phase::Recovering, _, false) => {
                next.phase = Phase::Active;
                next.since = now;
                next.deadline = None;
                Effects::default()
            }
            _ => Effects::default(),
        };
        Ok((next, effects))
    }

    /// One bounded timer transition. Condition timers win equal deadlines;
    /// expired notifications never become late notifications on a long tick.
    pub fn due(self, policy: Policy, now: i64) -> Result<(Self, Effects)> {
        policy.validate()?;
        if now < 0 || now < self.since {
            return Err(invalid("alarm time moved backwards"));
        }
        let mut next = self;
        let Some(deadline) = self.next_deadline(policy)?.filter(|at| *at <= now) else {
            return Ok((next, Effects::default()));
        };
        let effects = if self.deadline == Some(deadline) {
            match self.phase {
                Phase::Pending => next.activate(policy, now)?,
                Phase::Recovering => next.resolve(policy, now)?,
                _ => return Err(invalid("alarm condition timer has no pending transition")),
            }
        } else {
            let since = self
                .notification_since
                .ok_or_else(|| invalid("alarm notification timer missing"))?;
            next.notification_since = None;
            if add(since, policy.notification_max_age_micros)? <= now {
                Effects {
                    notification_expired: true,
                    ..Effects::default()
                }
            } else {
                next.not_before = add(now, policy.cooldown_micros)?;
                Effects {
                    event: Some(next.event(EventKind::Notify, true, now)),
                    ..Effects::default()
                }
            }
        };
        Ok((next, effects))
    }
}
