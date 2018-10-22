import { TOPICS } from "../entities";

import { selectEnvEntity } from "./state/envEntities";

export * from "./state";

export const selectTopics = (state, environment) =>
  selectEnvEntity(state, environment, TOPICS);
