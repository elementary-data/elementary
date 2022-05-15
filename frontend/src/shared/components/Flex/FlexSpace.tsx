import styled, { css } from 'styled-components';
import { flex } from './Flex';

export const flexSpace = css`
  ${flex};
  justify-content: space-between;
  align-items: center;
`;

export const FlexSpace = styled.div`
  ${flexSpace};
`;
