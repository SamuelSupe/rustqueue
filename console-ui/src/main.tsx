import React from 'react';
import ReactDOM from 'react-dom/client';
import '@carbon/charts-react/styles.css';
import App from './App';
import { I18nProvider } from './i18n';
import './styles.scss';

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <I18nProvider><App /></I18nProvider>
  </React.StrictMode>,
);
