import React from "react";

import { Link } from "react-router-dom";

const SECTIONS = [
  {
    title: "Consumers",
    description: "View and manage active kafka consumers managed by KDT",
    cta: { dest: "/manager/consumers", text: "Manage Consumers" }
  },
  {
    title: "Environments",
    description: "View kafka environemnts available to KDT",
    cta: { dest: "/manager/environments", text: "View Environments" }
  },
  {
    title: "Topics",
    description: "View kafka topics available to KDT",
    cta: { dest: "/manager/topics", text: "View Topics" }
  }
];

const ManagerSectionTile = ({ section }) => (
  <div className="card">
    {section.img &&
      section.img.url && (
        <img
          className="card-img-top"
          src={section.img.url}
          alt="Card image cap"
        />
      )}
    <div className="card-body">
      <h5 className="card-title">{section.title}</h5>
      <p className="card-text">{section.description}</p>
      {section.cta && (
        <Link to={section.cta.dest} className="btn btn-primary">
          {section.cta.text}
        </Link>
      )}
    </div>
  </div>
);

const ManagerHome = () => (
  <div className="ManagerHome">
    <h1>Kafka Dev Tools - Manager</h1>
    <div className="row">
      {SECTIONS.map((section, i) => (
        <div className="col-3" key={i}>
          <ManagerSectionTile section={section} />
        </div>
      ))}
    </div>
  </div>
);

export default ManagerHome;
