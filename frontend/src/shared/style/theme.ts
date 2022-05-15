import { createTheme } from '@mui/material';

const colors = {
  white: '#FFFFFF',
  black: '#273138',
  gray: '#E6E8EC',
  gray1: '#D4DCE7',
  gray2: '#6B747A',
};

const theme = createTheme({
  palette: {
    common: colors,
  },

  components: {
    MuiTypography: {
      defaultProps: {
        color: colors.black,
      },
    },
    MuiButton: {
      styleOverrides: {
        root: {
          textTransform: 'none',
        },
      },
    },
    MuiButtonGroup: {
      styleOverrides: {
        grouped: {
          background: colors.white,
          color: colors.gray2,
          borderColor: colors.gray1,
          ':hover': { background: colors.gray },
        },
      },
    },
  },
});

export default theme;
