use super::CellId;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum FeatureScope {
    Cell(CellId),
    CatalogShard(u64),
    Topic(String),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct FeatureActivation {
    pub scope: FeatureScope,
    pub feature_level: u64,
    pub activated_at_ms: i64,
    pub minimum_protocol_version: u32,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ScopedFeatureLevels {
    levels: Vec<FeatureActivation>,
}

impl ScopedFeatureLevels {
    pub fn activate(
        &mut self,
        activation: FeatureActivation,
        observed_protocol_floor: u32,
    ) -> Result<bool, String> {
        if activation.feature_level == 0
            || observed_protocol_floor < activation.minimum_protocol_version
        {
            return Err("feature scope has not reached its protocol floor".into());
        }
        if let Some(current) = self
            .levels
            .iter()
            .find(|current| current.scope == activation.scope)
        {
            if activation.feature_level < current.feature_level {
                return Err("feature levels are forward-only after activation".into());
            }
            if activation.feature_level == current.feature_level {
                return Ok(false);
            }
        }
        if let Some(current) = self
            .levels
            .iter_mut()
            .find(|current| current.scope == activation.scope)
        {
            *current = activation;
        } else {
            self.levels.push(activation);
        }
        Ok(true)
    }

    pub fn level(&self, scope: &FeatureScope) -> u64 {
        self.levels
            .iter()
            .find(|activation| &activation.scope == scope)
            .map_or(1, |activation| activation.feature_level)
    }

    pub fn activations(&self) -> impl Iterator<Item = &FeatureActivation> {
        self.levels.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feature_levels_are_scoped_and_forward_only() {
        let mut levels = ScopedFeatureLevels::default();
        let scope = FeatureScope::Cell(CellId(4));
        levels
            .activate(
                FeatureActivation {
                    scope: scope.clone(),
                    feature_level: 2,
                    activated_at_ms: 1,
                    minimum_protocol_version: 2,
                },
                2,
            )
            .unwrap();
        assert_eq!(levels.level(&scope), 2);
        assert!(levels
            .activate(
                FeatureActivation {
                    scope,
                    feature_level: 1,
                    activated_at_ms: 2,
                    minimum_protocol_version: 1,
                },
                2,
            )
            .is_err());
    }
}
