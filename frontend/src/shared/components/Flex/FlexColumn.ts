import styled, { css } from 'styled-components';
import { flex } from './Flex';

export const flexColumn = css`
  ${flex};
  flex-direction: column;
`;

export const FlexColumn = styled.div`
  ${flexColumn};
`;
