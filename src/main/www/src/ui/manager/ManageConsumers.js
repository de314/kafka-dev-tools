import React from "react";
import ManagerService from "../../services/ManagerService";
import { selectTopics } from "../../rdx/selectors";

import { compose } from "recompose";
import { connect } from "react-redux";

const ManageConsumers = ({ topics }) => (
  <div className="ManageConsumers">
    <table className="table table-hover table-striped">
      <thead>
        <tr>
          <th>Topic</th>
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
  }))
)(ManageConsumers);
