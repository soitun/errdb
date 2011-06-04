{application, errdb,
 [{description, "errdb"},
  {vsn, "2.0"},
  {modules, [
    errdb,
    errdb_app,
    errdb_sup
  ]},
  {registered, [errdb]},
  {mod, {errdb_app, []}},
  {env, []},
  {applications, [kernel, stdlib]}]}.
