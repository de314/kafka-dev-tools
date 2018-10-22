import store from "../rdx/store";
import axios from "axios";
import {
  fetchTopics,
  fetchTopicsSuccess,
  fetchTopicsFailure
} from "../rdx/actions";

export class ManagerService {
  constructor(baseUrl = "http://localhost:10000") {
    this.baseUrl = baseUrl;
  }
  fetchTopics(environment = "local") {
    store.dispatch(fetchTopics(environment));
    return axios
      .get(`${this.baseUrl}/api/v1/manager/topics?env=${environment}`)
      .then(res => {
        console.log({ res });
        store.dispatch(fetchTopicsSuccess(environment, res.data));
      })
      .catch(error => store.dispatch(fetchTopicsFailure(environment, error)));
  }
}
