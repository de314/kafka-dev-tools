import { combineReducers } from "redux";
import { reducer as toastr } from "react-redux-toastr";
import entities from "./entities";
import envEntities from "./envEntities";

export default combineReducers({
  entities,
  envEntities,
  toastr
});

export * from "./entities";
export * from "./envEntities";
