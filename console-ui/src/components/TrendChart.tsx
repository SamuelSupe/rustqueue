import { LineChart } from '@carbon/charts-react';
import { Alignments, ChartTheme, ScaleTypes, type LineChartOptions } from '@carbon/charts';
import { useEffect, useRef } from 'react';
import type { TrendSample } from '../api/types';
import { useI18n } from '../i18n';

export function TrendChart({ samples, dark }: { samples: TrendSample[]; dark: boolean }) {
  const { language, t } = useI18n();
  const chart = useRef<HTMLDivElement>(null);
  const data = samples.flatMap((sample) => [
    { group: t('overview.publishRate'), date: new Date(sample.at_ms), value: sample.publish_per_second },
    { group: t('overview.deliverRate'), date: new Date(sample.at_ms), value: sample.deliver_per_second },
  ]);
  const windowEnd = Math.max(...samples.map((sample) => sample.at_ms));
  const windowStart = windowEnd - 15 * 60 * 1000;
  const options: LineChartOptions = {
    theme: dark ? ChartTheme.G100 : ChartTheme.WHITE,
    locale: {
      code: language === 'zh' ? 'zh-CN' : 'en-US',
      translations: {
        group: t('chart.group'),
        total: t('chart.total'),
        tabularRep: {
          title: t('chart.table'),
          downloadAsCSV: t('chart.downloadCsv'),
        },
        toolbar: {
          exportAsCSV: t('chart.exportCsv'),
          exportAsJPG: t('chart.exportJpg'),
          exportAsPNG: t('chart.exportPng'),
          zoomIn: t('chart.zoomIn'),
          zoomOut: t('chart.zoomOut'),
          resetZoom: t('chart.resetZoom'),
          moreOptions: t('chart.moreOptions'),
          makeFullScreen: t('chart.fullscreen'),
          exitFullScreen: t('chart.exitFullscreen'),
          showAsTable: t('chart.showTable'),
        },
      },
    },
    height: '260px',
    grid: { x: { enabled: false }, y: { enabled: true } },
    points: { enabled: false },
    legend: { alignment: Alignments.CENTER },
    axes: {
      bottom: {
        mapsTo: 'date',
        scaleType: ScaleTypes.TIME,
        domain: [new Date(windowStart), new Date(windowEnd)],
        ticks: { number: 5 },
      },
      left: { mapsTo: 'value', scaleType: ScaleTypes.LINEAR, title: t('overview.rateUnit') },
    },
    curve: 'curveMonotoneX',
    tooltip: { showTotal: false },
  };
  useEffect(() => {
    const root = chart.current;
    if (!root) return undefined;
    const localizeLegend = () => root.querySelector('[data-name="legend-items"]')?.setAttribute('aria-label', t('chart.dataGroups'));
    localizeLegend();
    const observer = new MutationObserver(localizeLegend);
    observer.observe(root, { childList: true, subtree: true });
    return () => observer.disconnect();
  }, [language, t]);
  return <div ref={chart}><LineChart data={data} options={options} /></div>;
}
