import styled, { css } from 'styled-components';
import { flex } from './Flex';

export const flexCenter = css`
  ${flex};
  justify-content: center;
  align-items: center;
`;

export const FlexCenter = styled.div`
  ${flexCenter};
`;
