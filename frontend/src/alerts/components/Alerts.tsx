import { ButtonGroup, Button, Typography, Stack } from '@mui/material';
import { FlexColumn } from 'shared/components';

const Alerts = () => (
  <Stack sx={{ padding: 2 }}>
    <Typography sx={{ marginBottom: 5 }}>Alerts</Typography>
    <ButtonGroup>
      <Button>One</Button>
      <Button>Two</Button>
      <Button>Three</Button>
    </ButtonGroup>
  </Stack>
);

export default Alerts;
