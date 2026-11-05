# Apps live beside GeoRiva, not in it

## Status

accepted

## Context

GeoRiva serves gridded data. Making sense of that data takes an application: a forecast viewer, a drought monitor, a
national climate website. Each needs the same foundation and a very different experience, so the question of where
such applications live will be asked of every new one.

ADR 0028 already settled half of this: core holds generic mechanisms and a plugin holds everything shaped like its own
domain. `georiva-publisher-forti` shows that in practice, with its own public model name, variable mapping, admin and
`/api/forecast/` routes, and nothing about a point forecast written down in core. What had no answer is everything the
user sees, and where a whole application belongs when it spans all of these.

Who builds these applications shapes the answer. They are built mostly by regional and partner teams and used by
National Meteorological Services, many without development capacity of their own. Leaving implementors to work it out
alone only suits independent development teams, who are not the main audience.

"The app" is also not one thing. A drought monitor is anomaly products, severity logic and a user interface, and each
of those has a different natural home.

## Decision

**An app lives in its own repository, beside GeoRiva, and core contains no app code.**

**Each concern has one home**, found by asking in this order:

1. Is it only about how things look or behave on screen? It belongs in the **app**.
2. Would a client in another domain need it too? It belongs in **core**.
3. Does it hold domain meaning that needs storage, admin screens or access control? It belongs in a **plugin**.
4. Still unsure? Keep it in the plugin. Moving something into core later is easier than taking it out.

**An app that needs its own backend ships it as a plugin**, in the app's repository, with one version for both parts:

```
georiva-forecast-viewer/
├── plugin/    # domain meaning, inside GeoRiva, routes through ADR 0028
└── web/       # the user interface, talks to GeoRiva over HTTP only
```

The frontend never reaches past HTTP. The plugin runs in GeoRiva's process and may use core's models, and it inherits
core's tenancy and visibility rules by doing so.

**Generic features belong in a generic client.** Region analysis, time-series charts and collage maps are not specific
to any domain, so they do not justify a separate app on their own. A new app is warranted when a workflow does not fit
an existing client.

**The explorer stays in core.** Browsing datasets and opening a detail page per item shows what data exists and that it
is correct, for any dataset, so it passes the second question above. Helping people use that data for a purpose is an
app's job.

**GeoRiva accommodates apps through conventions, not machinery:** a documented pattern, an app template, and a directory
of known apps, each labelled by who maintains it.

| Label       | Maintained by                                                 |
|-------------|---------------------------------------------------------------|
| Official    | The GeoRiva team, released and tested against GeoRiva         |
| Integration | Another product's team, with GeoRiva support                  |
| Community   | Whoever built it, listed with no promises                     |

The template is written from the first official app once it works, not before it. Official apps are capped at one until
the project has a second maintainer.

The forecast viewer is the first official app. ClimWeb is the first integration: GeoRiva is intended to own the data and
geomanager the presentation, with no forced migration of existing instances.

## Alternatives considered

**Apps in core.** One install and one release, and rejected because every app would add its domain and interface to a
system all tenants and all other apps share. It is what ADR 0028 refused for a single forecast route.

**An app framework in core** (a registry, an Apps admin section, an install command). The smoothest experience for
partner teams, and rejected as apps-in-core by another route. It would also be designed from a single example.

**A plugin that carries its own UI inside GeoRiva.** The simplest install, and rejected as the default because a UI in
GeoRiva's process can reach internals, so it proves nothing about whether the public interfaces are enough.

**Documentation only.** Cheap, and rejected because it is the "work it out yourself" answer that does not serve partner
teams.

**Separate repositories for an app's plugin and frontend.** Lets another client use the plugin alone, and rejected
because compatibility between the two becomes a problem from day one. They can be split if a second client needs it.

**A config file in the frontend instead of a plugin.** Fastest to a demo, and rejected for anything deployed: an operator
could not add a model without a redeploy, and a hand-written list bypasses the visibility rules.

## Consequences

- Building an app will expose gaps in core, and generic ones belong in core. The forecast viewer already points at two:
  a two-band vector texture for wind particles, and a public way to find the latest complete model run.
- Domain read logic has no contract yet. The read-side analysis plugin contract is still open (architecture §11.1), so
  until it exists a plugin's read logic is a custom route under ADR 0028.
- An app with a plugin has two parts to deploy. Plugin installation (ADR 0035) carries Python packages and has no way to
  bring a frontend container, so `web/` needs its own deployment step for now.
  **Amended by ADR 0039** for half of this: a bundle committed into the plugin's `static/` folder is carried by the same
  install and now gets a page route of its own, so `web/` is no longer addressless. A container is still the answer for
  an app that needs one; nothing here forces that step on a static bundle.
- Core's Python interfaces become something plugins depend on, as well as its HTTP routes, and neither carries a
  stability promise yet. App builders are early collaborators who should expect breakage.
- An app's concept may share a name with another plugin's. Forti's "Model" is one collection × one window, and the
  forecast viewer's model is not, so the viewer takes a different term and stays separate from Forti until a second use
  shows what is truly shared.
- The rule applies going forward. Anything already in core that it would place elsewhere, such as the climatology recipe,
  is not moved by this decision.
