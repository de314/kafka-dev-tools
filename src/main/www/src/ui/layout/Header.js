import React from "react";

import { Link, NavLink } from "react-router-dom";

const Header = ({ match }) => (
  <div className="Header">
    {console.log({ match })}
    <nav className="navbar navbar-expand-lg navbar-light bg-light">
      <Link className="navbar-brand" to="/">
        Kafka Dev Tools
      </Link>
      <button
        className="navbar-toggler"
        type="button"
        data-toggle="collapse"
        data-target="#navbarSupportedContent"
        aria-controls="navbarSupportedContent"
        aria-expanded="false"
        aria-label="Toggle navigation"
      >
        <span className="navbar-toggler-icon" />
      </button>

      <div className="collapse navbar-collapse" id="navbarSupportedContent">
        <div className="navbar-nav mr-auto">
          <NavLink className="nav-item nav-link" to="/consumers">
            Consumer
          </NavLink>
          <NavLink className="nav-link" to="/producers">
            Producer
          </NavLink>
        </div>
        <div className="navbar-nav">
          <NavLink className="nav-link" to="/manager">
            Manager
          </NavLink>
          <NavLink className="nav-link" to="/settings">
            Settings
          </NavLink>
        </div>
      </div>
    </nav>
  </div>
);

export default Header;
