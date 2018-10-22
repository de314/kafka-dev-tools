import React from "react";
import ReactDOM from "react-dom";
import store from "./rdx/store";
import * as serviceWorker from "./serviceWorker";

import { HashRouter as Router } from "react-router-dom";
import App from "./App";
import { Provider } from "react-redux";
import { PersistGate } from "redux-persist/integration/react";
import ReduxToastr from "react-redux-toastr";

import { ManagerService } from "./services/ManagerService";

import "bootstrap/dist/css/bootstrap.min.css";
import "font-awesome/css/font-awesome.min.css";
import "react-redux-toastr/lib/css/react-redux-toastr.min.css";
import "./index.css";

new ManagerService().fetchTopics();

ReactDOM.render(
  <Router>
    <Provider store={store}>
      <div>
        <PersistGate loading={null} persistor={store.persistor}>
          <App />
        </PersistGate>
        <ReduxToastr
          timeOut={4000}
          newestOnTop={false}
          preventDuplicates
          position="top-left"
          transitionIn="fadeIn"
          transitionOut="fadeOut"
          progressBar
          closeOnToastrClick
        />
      </div>
    </Provider>
  </Router>,
  document.getElementById("root")
);

// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: http://bit.ly/CRA-PWA
serviceWorker.unregister();
