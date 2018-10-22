import React from "react";
import { ManagerService } from "../../services/ManagerService";
import { selectTopics } from "../../rdx/selectors";
import classnames from "classnames";

import { compose, withHandlers } from "recompose";
import { connect } from "react-redux";

const managerService = new ManagerService();

const ManageTopics = ({ topics, onRefresh }) => (
  <div className="ManageTopics">
    <div className="text-right">
      <button className="btn btn-success" onClick={() => onRefresh()}>
        <i className="fa fa-refresh" /> Refresh
      </button>
    </div>
    <table
      className={classnames("table table-hover table-striped", {
        "bg-light text-secondary": topics.loading || !topics.initialized
      })}
    >
      <thead>
        <tr>
          <th>Topic Name</th>
        </tr>
      </thead>
      <tbody>
        {topics.content.map((topic, i) => (
          <tr key={i}>
            <td>{topic}</td>
          </tr>
        ))}
      </tbody>
    </table>
  </div>
);

export default compose(
  connect(state => ({
    topics: selectTopics(state, "local")
  })),
  withHandlers({
    onRefresh: () => () => managerService.fetchTopics()
  })
)(ManageTopics);
