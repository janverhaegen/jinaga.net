﻿namespace Jinaga.Store.SQLite.Test.Models;

[FactType("Company")]
internal record Company() { }

[FactType("Department")]
internal record Department(Company company) { }

[FactType("Project")]
internal record Project(Department department) { }

[FactType("Project.Deleted")]
internal record ProjectDeleted(Project project) { }

[FactType("Project.Restored")]
internal record ProjectRestored(ProjectDeleted deleted) { }