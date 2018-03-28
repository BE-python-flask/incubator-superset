import {
  CHART_TYPE,
  COLUMN_TYPE,
  DIVIDER_TYPE,
  HEADER_TYPE,
  MARKDOWN_TYPE,
  ROW_TYPE,
  TABS_TYPE,
  TAB_TYPE,
} from './componentTypes';

import {
  NEW_CHART_ID,
  NEW_COLUMN_ID,
  NEW_DIVIDER_ID,
  NEW_HEADER_ID,
  NEW_MARKDOWN_ID,
  NEW_ROW_ID,
  NEW_TABS_ID,
  NEW_TAB_ID,
} from './constants';

export default {
  [NEW_CHART_ID]: CHART_TYPE, // @TODO we will have to encode real chart ids => type in the future
  [NEW_COLUMN_ID]: COLUMN_TYPE,
  [NEW_DIVIDER_ID]: DIVIDER_TYPE,
  [NEW_HEADER_ID]: HEADER_TYPE,
  [NEW_MARKDOWN_ID]: MARKDOWN_TYPE,
  [NEW_ROW_ID]: ROW_TYPE,
  [NEW_TABS_ID]: TABS_TYPE,
  [NEW_TAB_ID]: TAB_TYPE,
};
