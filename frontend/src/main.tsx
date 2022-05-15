import { ThemeProvider } from '@mui/material';
import { createRoot } from 'react-dom/client';
import { BrowserRouter as Router } from 'react-router-dom';
import { RecoilRoot } from 'recoil';
import GlobalStyle from 'shared/style/global';
import theme from 'shared/style/theme';
import 'shared/services/i18n';
import App from './app';

const root = createRoot(document.getElementById('root')!);

root.render(
  <RecoilRoot>
    <ThemeProvider theme={theme}>
      <Router>
        <GlobalStyle />
        <App />
      </Router>
    </ThemeProvider>
  </RecoilRoot>,
);
