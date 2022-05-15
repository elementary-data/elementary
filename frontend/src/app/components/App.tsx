import { Routes, Route, Navigate } from 'react-router-dom';
import Alerts from 'alerts';

const App = () => (
  <Routes>
    <Route path="/alerts" element={<Alerts />} />

    <Route path="*" element={<Navigate to="alerts" />} />
  </Routes>
);

export default App;
