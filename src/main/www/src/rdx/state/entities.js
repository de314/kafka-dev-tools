import _ from "lodash";
import {
  FETCH_ENTITIES,
  FETCH_ENTITIES_SUCCESS,
  FETCH_ENTITIES_FAILURE
} from "../actions";

const selectSlice = state => _.get(state, "entities");

const defaultEntitySlice = cubeClass => ({
  initialized: false,
  loading: false,
  content: [],
  pagination: {
    page: -1,
    limit: -1,
    total: -1
  },
  error: null
});

const defaultState = {};

export default (state = defaultState, action) => {
  switch (action.type) {
    default:
  }
  return state;
};
