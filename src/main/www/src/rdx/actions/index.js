import { TOPICS } from "../../entities";

const makeActionCreator = (type, ...argNames) => (...args) => {
  const action = { type };
  argNames.forEach((arg, index) => (action[argNames[index]] = args[index]));
  return action;
};

/* <<<<<<< ENTITIES >>>>>>>> */

export const FETCH_ENTITIES = "FETCH_ENTITIES";
export const fetchEntities = makeActionCreator(FETCH_ENTITIES, "entityType");

export const FETCH_ENTITIES_SUCCESS = "FETCH_ENTITIES_SUCCESS";
export const fetchEntitiesSuccess = makeActionCreator(
  FETCH_ENTITIES_SUCCESS,
  "entityType",
  "content"
);

export const FETCH_ENTITIES_FAILURE = "FETCH_ENTITIES_FAILURE";
export const fetchEntitiesFailure = makeActionCreator(
  FETCH_ENTITIES_FAILURE,
  "entityType",
  "error"
);

export const FETCH_ENV_ENTITIES = "FETCH_ENV_ENTITIES";
export const fetchEnvEntities = makeActionCreator(
  FETCH_ENV_ENTITIES,
  "environment",
  "entityType"
);

export const FETCH_ENV_ENTITIES_SUCCESS = "FETCH_ENV_ENTITIES_SUCCESS";
export const fetchEnvEntitiesSuccess = makeActionCreator(
  FETCH_ENV_ENTITIES_SUCCESS,
  "environment",
  "entityType",
  "content"
);

export const FETCH_ENV_ENTITIES_FAILURE = "FETCH_ENV_ENTITIES_FAILURE";
export const fetchEnvEntitiesFailure = makeActionCreator(
  FETCH_ENV_ENTITIES_FAILURE,
  "environment",
  "entityType",
  "error"
);

export const fetchTopics = environemnt => fetchEnvEntities(environemnt, TOPICS);
export const fetchTopicsSuccess = (environemnt, content) =>
  fetchEnvEntitiesSuccess(environemnt, TOPICS, content);
export const fetchTopicsFailure = (environemnt, content) =>
  fetchEnvEntitiesFailure(environemnt, TOPICS, content);
