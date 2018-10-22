import _ from "lodash";
import {
  FETCH_ENV_ENTITIES,
  FETCH_ENV_ENTITIES_SUCCESS,
  FETCH_ENV_ENTITIES_FAILURE
} from "../actions";

const selectSlice = state => _.get(state, "envEntities");

export const selectEnvEntity = (state, environment, entityType) => {
  const slice = _.defaultTo(selectSlice(state)[environment], {})[entityType];
  if (_.isNil(slice)) {
    return defaultEntitySlice();
  }
  return slice;
};

const defaultEntitySlice = () => ({
  initialized: false,
  loading: false,
  content: [],
  error: null
});

const defaultState = {};

export default (state = defaultState, action) => {
  switch (action.type) {
    case FETCH_ENV_ENTITIES: {
      let slice = _.defaultTo(state[action.environment], {})[action.entityType];
      if (_.isNil(slice)) {
        slice = defaultEntitySlice();
      }
      slice.loading = true;
      const newState = _.cloneDeep(state);
      _.set(newState, `${action.environment}.${action.entityType}`, slice);
      return newState;
    }

    case FETCH_ENV_ENTITIES_SUCCESS: {
      const envSlice = _.defaultTo(state[action.environment], {});
      envSlice[action.entityType] = {
        initialized: true,
        loading: false,
        content: action.content,
        error: null
      };
      return _.cloneDeep(state);
    }

    case FETCH_ENV_ENTITIES_FAILURE: {
      const envSlice = _.defaultTo(state[action.environment], {});
      envSlice[action.entityType] = {
        initialized: true,
        loading: false,
        content: [],
        error: action.error
      };
      return _.cloneDeep(state);
    }
    default:
  }
  return state;
};
