import { Grid, Column, SkeletonText, SkeletonPlaceholder, Tile } from '@carbon/react';
import { useI18n } from '../i18n';

export function LoadingState() {
  const { t } = useI18n();
  return (
    <main className="loading-state">
      <div className="loading-state__intro">
        <SkeletonText heading width="35%" />
        <SkeletonText width="55%" />
        <p>{t('common.loading')}</p>
        <span>{t('common.loadingHint')}</span>
      </div>
      <Grid fullWidth narrow>
        {[0, 1, 2, 3].map((item) => (
          <Column key={item} sm={4} md={4} lg={4}>
            <Tile className="metric-tile"><SkeletonPlaceholder className="loading-state__tile" /></Tile>
          </Column>
        ))}
      </Grid>
    </main>
  );
}
