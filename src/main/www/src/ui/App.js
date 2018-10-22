import React from "react";

import { Switch, Route } from "react-router-dom";
import Header from "./layout/Header";

import ManagerHome from "./manager/ManagerHome";
import ManageConsumers from "./manager/ManageConsumers";
import ManageTopics from "./manager/ManageTopics";

import NotFound from "./NotFound";

const App = ({ match }) => (
  <div className="App container">
    {console.log({ match })}
    <Header />
    <Switch>
      <Route exact path="/manager/consumers" component={ManageConsumers} />
      <Route exact path="/manager/topics" component={ManageTopics} />
      <Route path="/manager" component={ManagerHome} />

      <Route component={NotFound} />
    </Switch>
  </div>
);

export default App;
