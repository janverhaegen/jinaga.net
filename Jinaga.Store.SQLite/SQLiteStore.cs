using Jinaga.Facts;
using Jinaga.Pipelines;
using Jinaga.Products;
using Jinaga.Projections;
using Jinaga.Services;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using static Jinaga.Store.SQLite.SQLiteStore;


namespace Jinaga.Store.SQLite
{

    using FactByLabel = ImmutableDictionary<string, FactDescription>;

    public class SQLiteStore : IStore
    {

        private ConnectionFactory connFactory;

        public SQLiteStore()
        {
            string dbFolderName = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
            this.connFactory = new ConnectionFactory(dbFolderName + "\\jinaga.db");
        }


        Task<ImmutableList<Fact>> IStore.Save(FactGraph graph, CancellationToken cancellationToken)
        {

            if (graph.FactReferences.IsEmpty)
            {
                return Task.FromResult(ImmutableList<Fact>.Empty);
            }
            else
            {
                ImmutableList<Fact> newFacts = ImmutableList<Fact>.Empty;
                foreach (var factReference in graph.FactReferences)
                {
                    var fact = graph.GetFact(factReference);

                    connFactory.WithTxn(
                        (conn, id) =>
                            {
                                string sql;

                                // Select or insert into FactType table.  Gets a FactTypeId
                                sql = $@"
                                    SELECT fact_type_id 
                                    FROM fact_type 
                                    WHERE name = '{fact.Reference.Type}'
                                ";
                                var factTypeId = conn.ExecuteScalar<String>(sql);
                                if (factTypeId == "")
                                {
                                    sql = $@"
                                        INSERT OR IGNORE INTO fact_type (name) 
                                        VALUES ('{fact.Reference.Type}')
                                    ";
                                    conn.ExecuteNonQuery(sql);
                                    sql = $@"
                                        SELECT fact_type_id 
                                        FROM fact_type 
                                        WHERE name = '{fact.Reference.Type}'
                                    ";
                                    factTypeId = conn.ExecuteScalar<String>(sql);
                                }

                                // Select or insert into Fact table.  Gets a FactId
                                sql = $@"
                                    SELECT fact_id FROM fact 
                                    WHERE hash = '{fact.Reference.Hash}' AND fact_type_id = {factTypeId}
                                ";
                                var factId = conn.ExecuteScalar<String>(sql);
                                if (factId == "")
                                {
                                    newFacts = newFacts.Add(fact);
                                    string data = Fact.Canonicalize(fact.Fields, fact.Predecessors);
                                    sql = $@"
                                        INSERT OR IGNORE INTO fact (fact_type_id, hash, data) 
                                        VALUES ({factTypeId},'{fact.Reference.Hash}', '{data}' )
                                    ";
                                    conn.ExecuteNonQuery(sql);
                                    sql = $@"
                                        SELECT fact_id 
                                        FROM fact 
                                        WHERE hash = '{fact.Reference.Hash}' AND fact_type_id = {factTypeId}
                                    ";
                                    factId = conn.ExecuteScalar<String>(sql);

                                    // For each predecessor of the inserted fact ...
                                    foreach (var predecessor in fact.Predecessors)
                                    {
                                        // Select or insert into Role table.  Gets a RoleId
                                        sql = $@"
                                            SELECT role_id 
                                            FROM role 
                                            WHERE defining_fact_type_id = {factTypeId} AND name = '{predecessor.Role}'
                                        ";
                                        var roleId = conn.ExecuteScalar<String>(sql);
                                        if (roleId == "")
                                        {
                                            sql = $@"
                                                INSERT OR IGNORE INTO role (defining_fact_type_id, name) 
                                                VALUES ({factTypeId},'{predecessor.Role}')
                                            ";
                                            conn.ExecuteNonQuery(sql);
                                            sql = $@"
                                                SELECT role_id 
                                                FROM role 
                                                WHERE defining_fact_type_id = {factTypeId} AND name = '{predecessor.Role}'
                                            ";
                                            roleId = conn.ExecuteScalar<String>(sql);
                                        }

                                        // Insert into Edge and Ancestor tables
                                        string predecessorFactId;
                                        switch (predecessor)
                                        {
                                            case PredecessorSingle s:
                                                predecessorFactId = getFactId(conn, s.Reference);
                                                InsertEdge(conn, roleId, factId, predecessorFactId);
                                                InsertAncestors(conn, factId, predecessorFactId);
                                                break;
                                            case PredecessorMultiple m:
                                                foreach (var predecessorMultipleReference in m.References)
                                                {
                                                    predecessorFactId = getFactId(conn, predecessorMultipleReference);
                                                    InsertEdge(conn, roleId, factId, predecessorFactId);
                                                    InsertAncestors(conn, factId, predecessorFactId);
                                                }
                                                break;
                                            default:
                                                break;
                                        }
                                    };
                                }
                                return 0;
                            },
                        false
                    );

                }
                return Task.FromResult(newFacts);
            }

        }


        private string getFactId(ConnectionFactory.Conn conn, FactReference factReference)
        {
            string sql;

            sql = $@"
                SELECT fact_type_id 
                FROM fact_type 
                WHERE name = '{factReference.Type}'
            ";
            var factTypeId = conn.ExecuteScalar<String>(sql);

            sql = $@"
                SELECT fact_id 
                FROM fact 
                WHERE hash = '{factReference.Hash}' AND fact_type_id = {factTypeId}
            ";
            return conn.ExecuteScalar<String>(sql);
        }


        private void InsertAncestors(ConnectionFactory.Conn conn, string factId, string predecessorFactId)
        {
            string sql;

            sql = $@"
                INSERT OR IGNORE INTO ancestor 
                    (fact_id, ancestor_fact_id)
                SELECT '{factId}', '{predecessorFactId}'
                UNION
                SELECT '{factId}', ancestor_fact_id
                FROM ancestor
                WHERE fact_id = '{predecessorFactId}'
            ";
            conn.ExecuteNonQuery(sql);
        }


        private void InsertEdge(ConnectionFactory.Conn conn, string roleId, string successorFactId, string predecessorFactId)
        {
            string sql;

            sql = $@"
                INSERT OR IGNORE INTO edge (role_id, successor_fact_id, predecessor_fact_id) 
                VALUES ({roleId}, {successorFactId}, {predecessorFactId})
            ";
            conn.ExecuteNonQuery(sql);
        }


        Task<FactGraph> IStore.Load(ImmutableList<FactReference> references, CancellationToken cancellationToken)
        {

            if (references.IsEmpty)
            {
                return Task.FromResult(FactGraph.Empty);
            }
            else
            {
                var factsFromDb = connFactory.WithConn(
                    (conn, id) =>
                        {
                            string[] referenceValues = references.Select((f) => "('" + f.Hash + "', '" + f.Type + "')").ToArray();
                            string sql;
                            sql = $@"
                                SELECT f.hash, 
                                       f.data,
                                       t.name
                                FROM fact f 
                                JOIN fact_type t 
                                    ON f.fact_type_id = t.fact_type_id    
                                WHERE (f.hash,t.name) 
                                    IN (VALUES {String.Join(",", referenceValues)} )

                             UNION 

                                SELECT f2.hash, 
                                       f2.data,
                                       t2.name
                                FROM fact f1 
                                JOIN fact_type t1 
                                    ON 
                                        f1.fact_type_id = t1.fact_type_id    
                                            AND
                                        (f1.hash,t1.name) IN (VALUES {String.Join(",", referenceValues)} ) 
                                JOIN ancestor a 
                                    ON a.fact_id = f1.fact_id 
                                JOIN fact f2 
                                    ON f2.fact_id = a.ancestor_fact_id 
                                JOIN fact_type t2 
                                    ON t2.fact_type_id = f2.fact_type_id
                            ";

                            return conn.ExecuteQuery<FactFromDb>(sql);
                        },
                    true   //exponentional backoff
                );

                FactGraphBuilder fb = new FactGraphBuilder();

                foreach (Fact fact in factsFromDb.Deserialise())
                {
                    fb.Add(fact);
                }

                return Task.FromResult(fb.Build());
            }
        }


        public Task<string> LoadBookmark(string feed)
        {
            throw new NotImplementedException();
        }

        public Task<ImmutableList<FactReference>> ListKnown(ImmutableList<FactReference> factReferences)
        {
            throw new NotImplementedException();
        }

        public Task SaveBookmark(string feed, string bookmark)
        {
            throw new NotImplementedException();
        }

        public class FactFromDb
        {
            public string hash { get; set; }
            public string data { get; set; }
            public string name { get; set; }
        }

        internal class ResultSetFactfromDb
        {
            public string hash { get; set; }
            public int fact_Id { get; set; }
            public string data { get; set; }
        }




        Task<ImmutableList<Product>> IStore.Query(ImmutableList<FactReference> startReferences, Specification specification, CancellationToken cancellationToken)
        {
            var factTypes = LoadFactTypesFromSpecification(specification);
            var roles = LoadRolesFromSpecification(specification, factTypes);
            ResultComposer composer = ResultSqlFromSpecification(startReferences, specification, factTypes, roles);
            if (composer is null)
            {
                return Task.FromResult(ImmutableList<Product>.Empty);
            }
            SqlQueryTree sqlQueryTree = composer.GetSqlQueries();
            ResultSetTree resultSets = connFactory.WithConn(
                    (conn, id) =>
                    {
                        return ExecuteQueryTree(sqlQueryTree, conn);
                    },
                    false   //exponentional backoff
            );
            return Task.FromResult(composer.Compose(resultSets));
        }



        private ResultSetTree ExecuteQueryTree(SqlQueryTree sqlQueryTree, ConnectionFactory.Conn conn)
        {
            var sqlQuery = sqlQueryTree.SqlQuery;
            var dataRows = conn.ExecuteQueryRaw(sqlQueryTree.SqlQuery.Sql, sqlQueryTree.SqlQuery.Parameters.ToArray());
            var resultSet = dataRows.Select(dataRow =>
            {
                var resultSetRow = sqlQuery.Labels.Aggregate(ImmutableDictionary<int, ResultSetFact>.Empty,
                                                    (acc, next) =>
                {
                    var fact = new ResultSetFact();
                    fact.Hash = dataRow[$"hash{next.Index}"];
                    fact.factId = int.Parse(dataRow[$"id{next.Index}"]);
                    fact.Data = dataRow[$"data{next.Index}"];

                    return acc.Add(next.Index, fact);
                });

                return resultSetRow;
            });

            var resultSetTree =  new ResultSetTree();

            resultSetTree.ResultSet = resultSet.ToImmutableList();


            foreach (var childQuery in sqlQueryTree.ChildQueries)
            {
                var childResultSet = ExecuteQueryTree(childQuery.Value, conn);
                resultSetTree.ChildResultSets.Add(childQuery.Key, childResultSet);
            };


            //foreach (var childQuery in sqlQueryTree.ChildQueries)
            //{
            //    var childResultSet = ExecuteQueryTree(childQuery, conn);
            //    resultSetTree.ChildResultSets.Add(childQuery.Name, childResultSet);
            //};

            return resultSetTree;
        }



        #region MLP

        internal class ResultSetFact
        {
            public string Hash { get; set; }
            public int factId { get; set; }
            public string Data { get; set; }
        }

        internal class ResultSetTree
        {
            public ImmutableList<ImmutableDictionary<int, ResultSetFact>> ResultSet { get; set; }
            public ImmutableDictionary<string, ResultSetTree> ChildResultSets { get; set; }
        }


        internal class SpecificationLabel
        {
            public string Name { get; set; }
            public int Index { get; set; }
            public string Type { get; set; }
        }

        internal class SpecificationSqlQuery
        {
            public string Sql { get; set; }
            public ImmutableList<object> Parameters { get; set; }
            public ImmutableList<SpecificationLabel> Labels { get; set; }
        }

        internal class SqlQueryTree
        {
            public SpecificationSqlQuery SqlQuery { get; set; }
            public ImmutableDictionary<string, SqlQueryTree> ChildQueries { get; set; }
        }



        #endregion




        //internal class SpecificationLabel
        //{
        //    public string Name { get; set; }
        //    public int Index { get; set; }
        //    public string Type { get; set; }
        //}

        //internal class SpecificationSqlQuery
        //{
        //    public string Sql { get; set; }
        //    public ImmutableList<object> Parameters { get; set; }
        //    public ImmutableList<SpecificationLabel> Labels { get; set; }
        //}

        //internal class SqlQueryTree
        //{
        //    public SpecificationSqlQuery SqlQuery { get; set; }
        //    public ImmutableDictionary<string, SqlQueryTree> ChildQueries { get; set; }

        //}







        private IEnumerable<FactTypeFromDb> LoadFactTypesFromSpecification(Specification specification)
        {
            //TODO: Now we load all factTypes from the DB.  Optimize by caching, and by adding only the factTypes used in the specification
            var factTypeResult = connFactory.WithConn(
                    (conn, id) =>
                    {
                        string sql;
                        sql = $@"
                            SELECT fact_type_id , name 
                            FROM fact_type
                        ";
                        return conn.ExecuteQuery<FactTypeFromDb>(sql);
                    },
                    true   //exponentional backoff
                );
            return factTypeResult;
        }

        private IEnumerable<RoleFromDb> LoadRolesFromSpecification(Specification specification, object factTypes)
        {
            //TODO: Now we load all roles from the DB.  Optimize by caching, and by adding only the roles used in the specification
            var rolesResult = connFactory.WithConn(
                    (conn, id) =>
                    {
                        string sql;
                        sql = $@"
                            SELECT role_id, defining_fact_type_id, name
                            FROM role                                             
                        ";
                        return conn.ExecuteQuery<RoleFromDb>(sql);
                    },
                    true   //exponentional backoff
                );
            return rolesResult;
        }





        private ResultComposer ResultSqlFromSpecification(ImmutableList<FactReference> start, Specification specification, IEnumerable<FactTypeFromDb> factTypes, IEnumerable<RoleFromDb> roles)
        {
            var descriptionBuilder = new ResultDescriptionBuilder(factTypes, roles);
            var description = descriptionBuilder.buildDescription(start, specification);

            if (!description.QueryDescription.IsSatisfiable())
            {
                return null;
            }
            return CreateResultComposer(description, 0);
        }



        private ResultComposer CreateResultComposer(ResultDescription description, int parentFactIdLength)
        {
            var sqlQuery = description.QueryDescription.generateResultSqlQuery();
            var resultProjection = description.ResultProjection;
            var childResultComposers = description.ChildResultDescriptions
                .Where(c => c.QueryDescription.IsSatisfiable())
                .Select(c => new NamedResultComposer(c.Name,  CreateResultComposer(c, description.QueryDescription.OutputLength())))
                .ToImmutableList();
            return new ResultComposer(sqlQuery, resultProjection, parentFactIdLength, childResultComposers);
        }


        private class FactsFromDb
        {
            public string hash { get; set; }
            public int defining_fact_type_id { get; set; }
            public string data { get; set; }    
        }


        private class FactTypeFromDb
        {
            public int fact_type_id { get; set; }
            public string name { get; set; }
        }

        private class RoleFromDb
        {
            public int role_id { get; set; }
            public int defining_fact_type_id { get; set; }
            public string name { get; set; }
        }


        //private class ResultSetTree
        //{
        //    private object[] resultSet;
        //    private ImmutableList<NamedResultSetTree> childResultSets;

        //    public ResultSetTree(object[] resultSet, ImmutableList<NamedResultSetTree> childResultSets)
        //    {
        //        this.resultSet = resultSet;
        //        this.childResultSets = childResultSets;                            
        //    }
           
        //}


        //private class NamedResultSetTree
        //{
        //    public NamedResultSetTree(string name, ResultSetTree resultSetTree)
        //    {
        //        Name = name;
        //        ResultSetTree = resultSetTree;
        //    }

        //    public string Name { get; }
        //    public ResultSetTree ResultSetTree { get; }
        //}


        private class ResultComposer
        {

            private SpecificationSqlQuery sqlQuery;
            private Projection resultProjection;
            private int parentFactIdLength;
            private ImmutableList<NamedResultComposer> childResultComposers;

            public ResultComposer(SpecificationSqlQuery sqlQuery, Projection resultProjection, int parentFactIdLength, ImmutableList<NamedResultComposer> childResultComposers)
            {
                this.sqlQuery = sqlQuery;
                this.resultProjection = resultProjection;
                this.parentFactIdLength = parentFactIdLength;
                this.childResultComposers = childResultComposers;
            }

            public SqlQueryTree GetSqlQueries()
            {
                var childQueries =  ImmutableList<NamedSqlQueryTree>.Empty;

                //foreach (var childResultComposer in childResultComposers)
                //{
                //    childQueries = childQueries.Add(new NamedSqlQueryTree(childResultComposer.Name,));
                //}

                return new SqlQueryTree(sqlQuery, childQueries);
            }

            public ImmutableList<Product> Compose(ResultSetTree resultSets)
            {
                throw new NotImplementedException();
            }
        }


        private class NamedResultComposer
        {
            public NamedResultComposer(string name, ResultComposer resultComposer)
            {
                Name = name;
                ResultComposer = resultComposer;
            }

            public string Name  { get; }
            public ResultComposer ResultComposer { get;}
        }


        private class ResultDescriptionBuilder
        {
            private IEnumerable<FactTypeFromDb> factTypes;
            private IEnumerable<RoleFromDb> roles;


            public ResultDescriptionBuilder(IEnumerable<FactTypeFromDb> factTypes, IEnumerable<RoleFromDb> roles)
            {
                this.factTypes = factTypes;
                this.roles = roles;
            }


            public ResultDescription buildDescription(ImmutableList<FactReference> start, Specification specification)
            {
                if (start.Count != specification.Given.Count)
                {
                    throw new Exception($"The number of start facts ({start.Count}) does not equal the number of inputs(${specification.Given.Count})");
                }

                for (int i = 0; i < start.Count; i++)
                {
                    if (start[i].Type != specification.Given[i].Type)
                    {
                        throw new Exception($"The type of start fact ${i}(${start[i].Type}) does not match the type of input ${i} (${specification.Given[i].Type})");
                    }
                }

                var initialQueryDescription = new QueryDescription(ImmutableList<InputDescription>.Empty, ImmutableList<object>.Empty, ImmutableList<OutputDescription>.Empty, ImmutableList<FactDescription>.Empty, ImmutableList<EdgeDescription>.Empty, ImmutableList<ExistentialConditionDescription>.Empty);
                var localProjection = SpecificationToLocalProjection(specification.Projection);
                return CreateResultDescription(initialQueryDescription, specification.Given, start, specification.Matches, localProjection, FactByLabel.Empty, Array.Empty<int>());
            }


            private Projection SpecificationToLocalProjection(Projections.Projection specificationProjection)
            {
                if (specificationProjection is Projections.FieldProjection specificationFieldProjection)
                {
                    return new FieldProjection(specificationFieldProjection.Tag, specificationFieldProjection.FieldName);

                }
                else
                {
                    throw new NotImplementedException();
                }
            }


            private ResultDescription CreateResultDescription(QueryDescription queryDescription, ImmutableList<Label> given, ImmutableList<FactReference> start, ImmutableList<Match> matches, Projection projection, FactByLabel knownFacts, int[] path)
            {
                (queryDescription, knownFacts) = AddEdges(queryDescription, given, start, knownFacts, path, matches);
                //if (!queryDescription.IsSatisfiable())
                //{
                //    return new ResultDescription(queryDescription, ImmutableList<NamedResultDescription>.Empty);
                //}
                //TODO : implement
                return new ResultDescription(queryDescription, projection, ImmutableList<NamedResultDescription>.Empty);
            }


            private (QueryDescription, FactByLabel) AddEdges(QueryDescription queryDescription, ImmutableList<Label> given, ImmutableList<FactReference> start, FactByLabel knownFacts, int[] path, ImmutableList<Match> matches)
            {
                foreach (var match in matches)
                {
                    foreach (var condition in match.Conditions)
                    {
                        if (condition is PathCondition pathCondition)
                        {
                            (queryDescription, knownFacts) = addPathCondition(queryDescription, given, start, knownFacts, path, match.Unknown, "", pathCondition);
                        }
                        else if (condition is ExistentialCondition existentialCondition)
                        {
                            throw new NotImplementedException();
                        }
                        if (!queryDescription.IsSatisfiable())
                        {
                            break;
                        }

                    }
                    if (!queryDescription.IsSatisfiable())
                    {
                        break;
                    }

                }
                return (queryDescription, knownFacts);
            }


            private (QueryDescription, FactByLabel) addPathCondition(QueryDescription queryDescription, ImmutableList<Label> given, ImmutableList<FactReference> start, FactByLabel knownFacts, int[] path, Label unknown, string prefix, PathCondition condition)
            {
                if (!knownFacts.ContainsKey(condition.LabelRight))
                {
                    var givenIndex = given.FindIndex(l => l.Name == condition.LabelRight);
                    if (givenIndex < 0)
                    {
                        throw new Exception($"No input parameter found for label {condition.LabelRight}");
                    }
                    FactDescription factDescription = new FactDescription();
                    (queryDescription, factDescription) = queryDescription.withInputParameter(given[givenIndex],
                                                                                              ensureGetFactTypeId(factTypes, start[givenIndex].Type),
                                                                                              start[givenIndex].Hash,
                                                                                              path);
                    knownFacts = knownFacts.Add(condition.LabelRight, factDescription);
                }

                var knownFact = knownFacts.GetValueOrDefault(unknown.Name);
                var roleCount = condition.RolesLeft.Count + condition.RolesRight.Count;

                var fact = knownFacts.GetValueOrDefault(condition.LabelRight);
                if (fact == null)
                {
                    throw new Exception($"Label {condition.LabelRight} not found. Known labels: {knownFacts.Keys.JoinStrings(",")}");
                }
                var type = fact.Type;
                var factIndex = fact.FactIndex;

                //TODO onderstaande loop implementeren
                //for (const [i, role] of condition.rolesRight.entries()) {
                //    ......
                //}

                var rightType = type;
                type = unknown.Type;



                var newEdges = ImmutableList<(int roleId, string declaringType)>.Empty;


                foreach (var role in condition.RolesLeft)
                {
                    var typeId = getFactTypeId(factTypes, type);
                    if (typeId == null)
                    {
                        return (QueryDescription.unsatisfiable(), knownFacts);
                    }

                    var roleId = getRoleId(roles, (int)typeId, role.Name);
                    if (roleId == null)
                    {
                        return (QueryDescription.unsatisfiable(), knownFacts);
                    }

                    newEdges = newEdges.Add(((int)roleId, type));
                    type = role.TargetType;
                }

                if (type != rightType)
                {
                    throw new Exception($"Type mismatch: {type} is compared to {rightType}");
                }


                var a = newEdges.Reverse().Select((e, i) =>
                    {
                        (QueryDescription queryWithParameter, int roleParameter) = queryDescription.withParameters(e.roleId);
                        if ((condition.RolesRight.Count + 1 == roleCount - 1) && !(knownFact == null))
                        {
                            throw new NotImplementedException();
                        }
                        else
                        {
                            (QueryDescription queryWithFact, int successorFactIndex) = queryWithParameter.withFact(e.declaringType);
                            queryDescription = queryWithFact.withEdge(factIndex, successorFactIndex, roleParameter, path);
                            factIndex = successorFactIndex;
                        }
                        return e;
                    }
                ).Count();

                if (!knownFacts.IsEmpty)
                {
                    knownFacts.Add(unknown.Name, new FactDescription(unknown.Type, factIndex));
                    if (path.Length == 0)
                    {
                        queryDescription = queryDescription.withOutput(prefix + unknown.Name, unknown.Type, factIndex);
                    }
                }
                return (queryDescription, knownFacts);
            }


            private int? getRoleId(IEnumerable<RoleFromDb> roles, int typeId, string name)
            {
                return roles.Where(r => r.defining_fact_type_id == typeId && r.name == name).Select(r => (int?)r.role_id).FirstOrDefault();
            }

            private int? getFactTypeId(IEnumerable<FactTypeFromDb> factTypes, string name)
            {
                return factTypes.Where(n => n.name == name).Select(n => (int?)n.fact_type_id).FirstOrDefault();
            }

            private int ensureGetFactTypeId(IEnumerable<FactTypeFromDb> factTypes, string name)
            {
                return factTypes.Where(n => n.name == name).Select(n => n.fact_type_id).First();
            }


          




        }



        private class Projection
        {

        }

        private class FieldProjection : Projection
        {
            public string Label { get; set; }
            public string Field { get; set; }

            public FieldProjection(string label, string field)
            {
                Label = label;
                Field = field;
            }
        }



        private class ResultDescription
        {

            public QueryDescription QueryDescription { get; }
            
            public Projection ResultProjection { get; }

            public ImmutableList<NamedResultDescription> ChildResultDescriptions { get; }

            public ResultDescription(QueryDescription queryDescription, Projection resultProjection, ImmutableList<NamedResultDescription> childResultDescriptions)
            {
                QueryDescription = queryDescription;
                ChildResultDescriptions = childResultDescriptions;
                ResultProjection= resultProjection; 
            }

        }



        private class NamedResultDescription : ResultDescription
        {
            public NamedResultDescription(QueryDescription queryDescription, Projection resultProjection, ImmutableList<NamedResultDescription> childResultDescriptions, string name) : base(queryDescription, resultProjection, childResultDescriptions)
            {
                Name = name;
            }

            public String Name { get; set; }
        }


        private class QueryDescription
        {

            public QueryDescription(ImmutableList<InputDescription> inputs,
                                    ImmutableList<object> parameters,
                                    ImmutableList<OutputDescription> outputs,
                                    ImmutableList<FactDescription> facts,
                                    ImmutableList<EdgeDescription> edges,
                                    ImmutableList<ExistentialConditionDescription> existentialConditions)
            {
                Inputs = inputs;
                Parameters = parameters;
                Outputs = outputs;
                Facts = facts;
                Edges = edges;
                ExistentialConditions = existentialConditions;
            }


            public static QueryDescription unsatisfiable()
            {
                return new QueryDescription(ImmutableList<InputDescription>.Empty, ImmutableList<object>.Empty, ImmutableList<OutputDescription>.Empty, ImmutableList<FactDescription>.Empty, ImmutableList<EdgeDescription>.Empty, ImmutableList<ExistentialConditionDescription>.Empty);
            }


            private ImmutableList<InputDescription> Inputs { get; }
            private ImmutableList<object> Parameters { get; }
            private ImmutableList<OutputDescription> Outputs { get; }
            private ImmutableList<FactDescription> Facts { get; }
            private ImmutableList<EdgeDescription> Edges { get; }
            private ImmutableList<ExistentialConditionDescription> ExistentialConditions { get; }

            internal bool IsSatisfiable()
            {
                return this.Outputs.Count > 0;
            }


            internal (QueryDescription, FactDescription) withInputParameter(Label label, int factTypeId, string factHash, int[] path)
            {
                var factTypeParameter = Parameters.Count + 1;
                var factHashParameter = factTypeParameter + 1;
                var factIndex = Facts.Count + 1;
                var factDescription = new FactDescription(label.Type, factIndex);
                var facts = Facts.Add(factDescription);
                var input = new InputDescription(label.Name, factIndex, factTypeParameter, factHashParameter);
                var parameters = Parameters.Add(factTypeId).Add(factHash);
                if (path.Length == 0)
                {
                    var inputs = Inputs.Add(input);
                    var queryDescription = new QueryDescription(inputs, parameters, Outputs, facts, Edges, ExistentialConditions);
                    return (queryDescription, factDescription);
                }
                else
                {
                    var existentialConditions = existentialsWithInput(ExistentialConditions, input, path);
                    var queryDescription = new QueryDescription(Inputs, parameters, Outputs, facts, Edges, existentialConditions);
                    return (queryDescription, factDescription);
                }
            }


            internal QueryDescription withOutput(string label, string type, int factIndex)
            {
                var output = new OutputDescription(label, type, factIndex);
                var queryDescription = new QueryDescription(Inputs, Parameters, Outputs.Add(output), Facts, Edges, ExistentialConditions);
                return queryDescription;
            }


            internal (QueryDescription query, int parameterIndex) withParameters(int parameter)
            {
                var parameterIndex = Parameters.Count + 1;
                var query = new QueryDescription(Inputs, Parameters.Add(parameter), Outputs, Facts, Edges, ExistentialConditions);
                return (query, parameterIndex);
            }


            internal (QueryDescription query, int factIndex) withFact(string type)
            {
                var factIndex = Facts.Count + 1;
                var fact = new FactDescription(type, factIndex);
                var query = new QueryDescription(Inputs, Parameters, Outputs, Facts.Add(fact), Edges, ExistentialConditions);
                return (query, factIndex);
            }


            internal QueryDescription withEdge(int predecessorFactIndex, int successorFactIndex, int roleParameter, int[] path)
            {
                var edge = new EdgeDescription(Edges.Count + countEdges(ExistentialConditions) + 1,
                                               predecessorFactIndex,
                                               successorFactIndex,
                                               roleParameter);
                var query = (path.Length == 0)
                    ? new QueryDescription(Inputs, Parameters, Outputs, Facts, Edges.Add(edge), ExistentialConditions)
                    : new QueryDescription(Inputs, Parameters, Outputs, Facts, Edges, existentialsWithEdge(ExistentialConditions, edge, path));
                return query;
            }


            private ImmutableList<ExistentialConditionDescription> existentialsWithEdge(ImmutableList<ExistentialConditionDescription> existentialConditions, EdgeDescription edge, int[] path)
            {
                if (path.Length == 1)
                {
                    return (ImmutableList<ExistentialConditionDescription>)existentialConditions.Select(
                        (c, index) =>
                            index == path[0]
                            ? new ExistentialConditionDescription(c.Exists, c.Inputs, c.Edges.Add(edge), c.ExistentialConditions)
                            : c
                        );
                }
                else
                {
                    return (ImmutableList<ExistentialConditionDescription>)existentialConditions.Select(
                        (c, index) =>
                            index == path[0]
                            ? new ExistentialConditionDescription(c.Exists, c.Inputs, c.Edges, existentialsWithEdge(c.ExistentialConditions, edge, path[1..]))
                            : c
                        );
                }
            }


            private ImmutableList<ExistentialConditionDescription> existentialsWithInput(ImmutableList<ExistentialConditionDescription> existentialConditions, InputDescription input, int[] path)
            {
                if (path.Length == 1)
                {
                    return (ImmutableList<ExistentialConditionDescription>)existentialConditions.Select(
                        (c, index) =>
                            index == path[0]
                            ? new ExistentialConditionDescription(c.Exists, c.Inputs.Add(input), c.Edges, c.ExistentialConditions)
                            : c
                        );
                }
                else
                {
                    return (ImmutableList<ExistentialConditionDescription>)existentialConditions.Select(
                        (c, index) =>
                            index == path[0]
                            ? new ExistentialConditionDescription(c.Exists, c.Inputs, c.Edges, existentialsWithInput(c.ExistentialConditions, input, path[1..]))
                            : c
                        );
                }
            }


            private int countEdges(ImmutableList<ExistentialConditionDescription> existentialConditions)
            {
                return existentialConditions.Aggregate(0,
                                                       (count, existentialCondition) =>
                                                       count + existentialCondition.Edges.Count + countEdges(existentialCondition.ExistentialConditions)
                );
            }

            internal SpecificationSqlQuery generateResultSqlQuery()
            {
                var columns = Outputs.Select(o => $"f{o.FactIndex}.hash as hash{o.FactIndex}, f{o.FactIndex}.fact_id as id{o.FactIndex}, f{o.FactIndex}.data as data{o.FactIndex}")
                                     .JoinStrings(",");

                var firstEdge = Edges[0];
                var predecessorFact = Inputs.Find(i => i.FactIndex == firstEdge.PredecessorFactIndex);
                var successorFact = Inputs.Find(i => i.FactIndex == firstEdge.SuccessorFactIndex);
                var firstFactIndex = (predecessorFact != null) ? predecessorFact.FactIndex : successorFact.FactIndex;
                var writtenFactIndexes = ImmutableList<int>.Empty.Add(firstFactIndex);
                var joins = GenerateJoins(Edges, writtenFactIndexes);
                var inputWhereClauses = Inputs.Select(i => $"f{i.FactIndex}.fact_type_id = ${i.FactTypeParameter} AND f{i.FactIndex}.hash = ${i.FactHashParameter}")
                                              .JoinStrings(" AND ");
                var existentialWhereClauses = ExistentialConditions.Select(e => $" AND {(e.Exists ? "EXISTS" : "NOT EXISTS")}  {generateExistentialWhereClause(e, writtenFactIndexes)} ")
                                                                   .JoinStrings("");
                var orderByClause = Outputs.Select(o => $"f{o.FactIndex}.fact_id ASC")
                                           .JoinStrings(",");                
                var sql = $"SELECT {columns} FROM fact f{firstFactIndex}{joins.JoinStrings("")} WHERE {inputWhereClauses}{existentialWhereClauses} ORDER BY {orderByClause}";
                return new SpecificationSqlQuery(sql, Parameters, Outputs.Select(o => new SpecificationLabel(o.FactIndex, o.Label)).ToImmutableList(), "");
            }


            private ImmutableList<String> GenerateJoins(ImmutableList<EdgeDescription> edges, ImmutableList<int> writtenFactIndexes)
            {
                var joins = ImmutableList<String>.Empty;
                edges.ForEach(e =>
                    {
                        if (writtenFactIndexes.Contains(e.PredecessorFactIndex))
                        {
                            if (writtenFactIndexes.Contains(e.SuccessorFactIndex))
                            {
                                joins = joins.Add(
                                    $" JOIN edge e{e.EdgeIndex}" +
                                    $" ON e{e.EdgeIndex}.predecessor_fact_id = f{e.PredecessorFactIndex}.fact_id" +
                                    $" AND e{e.EdgeIndex}.successor_fact_id = f{ e.SuccessorFactIndex}.fact_id" +
                                    $" AND e{e.EdgeIndex}.role_id = ${e.RoleParameter}"                                                                            
                                );
                            }
                            else
                            {
                                joins = joins.Add(
                                    $" JOIN edge e{e.EdgeIndex}" +
                                    $" ON e{e.EdgeIndex}.predecessor_fact_id = f{e.PredecessorFactIndex}.fact_id" +
                                    $" AND e{e.EdgeIndex}.role_id = ${e.RoleParameter}"
                                );
                                joins = joins.Add(
                                    $" JOIN fact f{e.SuccessorFactIndex}" +
                                    $" ON f{e.SuccessorFactIndex}.fact_id = e{e.EdgeIndex}.successor_fact_id"
                                );
                                writtenFactIndexes.Add(e.SuccessorFactIndex);
                            }                            
                        }
                        else if (writtenFactIndexes.Contains(e.SuccessorFactIndex))
                        {
                            joins = joins.Add(
                                $" JOIN edge e{e.EdgeIndex}" +
                                $" ON e{e.EdgeIndex}.successor_fact_id = f{e.SuccessorFactIndex}.fact_id" +
                                $" AND e{e.EdgeIndex}.role_id = ${e.RoleParameter}"
                            );
                            joins = joins.Add(
                                $" JOIN fact f{e.PredecessorFactIndex}" +
                                $" ON f{e.PredecessorFactIndex}.fact_id = e{e.EdgeIndex}.predecessor_fact_id"
                            );
                            writtenFactIndexes.Add(e.PredecessorFactIndex);                            
                        }
                        else
                        {
                            throw new Exception("Neither predecessor nor successor fact has been written");
                        }
                    }
                );
                return joins;
            }


            private String generateExistentialWhereClause(ExistentialConditionDescription existentialCondition, ImmutableList<int> outerFactIndexes)
            {
                var firstEdge = existentialCondition.Edges.First();
                var writtenFactIndexes = ImmutableList<int>.Empty.AddRange(outerFactIndexes);
                var firstJoin = ImmutableList<String>.Empty;
                var whereClause = ImmutableList<String>.Empty;
                if (writtenFactIndexes.Contains(firstEdge.PredecessorFactIndex))
                {
                    if (writtenFactIndexes.Contains(firstEdge.SuccessorFactIndex))
                    {
                        throw new NotImplementedException();
                    }
                    else
                    {
                        whereClause = whereClause.Add(
                            $"e{ firstEdge.EdgeIndex}.predecessor_fact_id = f{ firstEdge.PredecessorFactIndex}.fact_id" +
                            $" AND e{ firstEdge.EdgeIndex}.role_id = ${ firstEdge.RoleParameter}"
                        );
                        firstJoin = firstJoin.Add(
                            $" JOIN fact f{firstEdge.SuccessorFactIndex}" +
                            $" ON f{firstEdge.SuccessorFactIndex}.fact_id = e{firstEdge.EdgeIndex}.successor_fact_id"
                        );
                        writtenFactIndexes= writtenFactIndexes.Add(firstEdge.SuccessorFactIndex);
                    }                    
                }
                else if (writtenFactIndexes.Contains(firstEdge.SuccessorFactIndex))
                {
                    whereClause= whereClause.Add(
                        $"e{ firstEdge.EdgeIndex}.successor_fact_id = f{ firstEdge.SuccessorFactIndex}.fact_id" +
                        $" AND e{ firstEdge.EdgeIndex}.role_id = ${ firstEdge.RoleParameter}"
                    );

                    firstJoin = firstJoin.Add(
                        $" JOIN fact f{firstEdge.PredecessorFactIndex}" +
                        $" ON f{firstEdge.PredecessorFactIndex}.fact_id = e{firstEdge.EdgeIndex}.predecessor_fact_id"
                    );
                    writtenFactIndexes = writtenFactIndexes.Add(firstEdge.PredecessorFactIndex);
                }
                else
                {
                    throw new Exception("Neither predecessor nor successor fact has been written");
                }
                var tailJoins = GenerateJoins(existentialCondition.Edges.Take(1).ToImmutableList(),writtenFactIndexes);
                var joins = firstJoin.AddRange(tailJoins);  
                var inputWhereClauses = existentialCondition.Inputs
                    .Select(i =>  $" AND f{i.FactIndex}.fact_type_id = ${i.FactTypeParameter} AND f{i.FactIndex}.hash = ${i.FactHashParameter}")
                    .JoinStrings("");
                var existentialWhereClauses = existentialCondition.ExistentialConditions
                    .Select(e => $" AND {(e.Exists ? "EXISTS" : "NOT EXISTS")} ({ generateExistentialWhereClause(e, writtenFactIndexes)})")
                    .JoinStrings("");
                return $"SELECT 1 FROM edge e{firstEdge.EdgeIndex}{joins.JoinStrings("")} WHERE {whereClause.JoinStrings(" AND ")}{ inputWhereClauses}{ existentialWhereClauses}";
            }

            internal int OutputLength()
            {
                return Outputs.Count;
            }
        }



        private class InputDescription
        {
            public InputDescription(string label, int factIndex, int factTypeParameter, int factHashParameter)
            {
                Label = label;
                FactIndex = factIndex;
                FactTypeParameter = factTypeParameter;
                FactHashParameter = factHashParameter;
            }

            public string Label { get; set; }
            public int FactIndex { get; set; }
            public int FactTypeParameter { get; set; }
            public int FactHashParameter { get; set; }
        }


        private class OutputDescription
        {
            public OutputDescription(string label, string type, int factIndex)
            {
                Label = label;
                Type = type;
                FactIndex = factIndex;
            }

            public string Label { get; set; }
            public string Type { get; set; }
            public int FactIndex { get; set; }
        }


        public class FactDescription
        {
            public FactDescription() { }
            public FactDescription(string type, int factIndex)
            {
                Type = type;
                FactIndex = factIndex;
            }

            public string Type { get; }
            public int FactIndex { get; }
        }


        private class EdgeDescription
        {
            public EdgeDescription(int edgeIndex, int predecessorFactIndex, int successorFactIndex, int roleParameter)
            {
                EdgeIndex = edgeIndex;
                PredecessorFactIndex = predecessorFactIndex;
                SuccessorFactIndex = successorFactIndex;
                RoleParameter = roleParameter;
            }

            public int EdgeIndex { get; set; }
            public int PredecessorFactIndex { get; set; }
            public int SuccessorFactIndex { get; set; }
            public int RoleParameter { get; set; }
        }


        private class ExistentialConditionDescription
        {
            public ExistentialConditionDescription(bool exists, ImmutableList<InputDescription> inputs, ImmutableList<EdgeDescription> edges, ImmutableList<ExistentialConditionDescription> existentialConditions)
            {
                Exists = exists;
                Inputs = inputs;
                Edges = edges;
                ExistentialConditions = existentialConditions;
            }

            public bool Exists { get; set; }
            public ImmutableList<InputDescription> Inputs { get; set; }
            public ImmutableList<EdgeDescription> Edges { get; set; }
            public ImmutableList<ExistentialConditionDescription> ExistentialConditions { get; set; }
        }




        //private class FactByLabel : ImmutableDictionary<string, FactDescription>
        //{

        //}


    }





    //internal class SqlQueryTree
    //{
    //    public SqlQueryTree(SpecificationSqlQuery sqlQuery, ImmutableList<NamedSqlQueryTree> childQueries)
    //    {
    //        SqlQuery = sqlQuery;
    //        ChildQueries = childQueries;
    //    }

    //    public SpecificationSqlQuery SqlQuery { get; }
    //    public ImmutableList<NamedSqlQueryTree> ChildQueries { get; }
    //}

    //internal class NamedSqlQueryTree : SqlQueryTree
    //{
    //    public NamedSqlQueryTree(String name, SpecificationSqlQuery sqlQuery, ImmutableList<NamedSqlQueryTree> childQueries) : base(sqlQuery, childQueries)
    //    {
    //        Name = name;
    //    }

    //    public string Name { get; }
    //}


    //internal class SpecificationSqlQuery
    //{
    //    public SpecificationSqlQuery(string sql, ImmutableList<object> parameters, ImmutableList<SpecificationLabel> labels, string bookmark)
    //    {
    //        Sql = sql;
    //        Parameters = parameters;
    //        Labels = labels;
    //        Bookmark = bookmark;
    //    }

    //    public string Sql { get; }

    //    public ImmutableList<object> Parameters { get; }

    //    public ImmutableList<SpecificationLabel> Labels { get; }

    //    public string Bookmark { get; }

    //}


    //internal class SpecificationLabel
    //{
    //    public SpecificationLabel(int index, string name)
    //    {
    //        Index = index;
    //        Name = name;
    //    }

    //    public int Index { get; }

    //    public string Name { get; }
    //}











    public static class MyExtensions
    {

        public static string JoinStrings<TItem>(this IEnumerable<TItem> enumerable, string separator)
        {
            return string.Join(separator, enumerable);
        }


        public static IEnumerable<Fact> Deserialise(this IEnumerable<FactFromDb> factsFromDb)
        {

            foreach (var FactFromDb in factsFromDb)
            {
                ImmutableList<Field> fields = ImmutableList<Field>.Empty;
                ImmutableList<Predecessor> predecessors = ImmutableList<Predecessor>.Empty;

                using (JsonDocument document = JsonDocument.Parse(FactFromDb.data))
                {
                    JsonElement root = document.RootElement;

                    JsonElement fieldsElement = root.GetProperty("fields");
                    foreach (var field in fieldsElement.EnumerateObject())
                    {
                        switch (field.Value.ValueKind)
                        {
                            case JsonValueKind.String:
                                fields = fields.Add(new Field(field.Name, new FieldValueString(field.Value.GetString())));
                                break;
                            case JsonValueKind.Number:
                                fields = fields.Add(new Field(field.Name, new FieldValueNumber(field.Value.GetDouble())));
                                break;
                            case JsonValueKind.True:
                            case JsonValueKind.False:
                                fields = fields.Add(new Field(field.Name, new FieldValueBoolean(field.Value.GetBoolean())));
                                break;
                        }
                    }

                    string hash;
                    string type;
                    JsonElement predecessorsElement = root.GetProperty("predecessors");
                    foreach (var predecessor in predecessorsElement.EnumerateObject())
                    {
                        switch (predecessor.Value.ValueKind)
                        {
                            case JsonValueKind.Object:
                                hash = predecessor.Value.GetProperty("hash").GetString();
                                type = predecessor.Value.GetProperty("type").GetString();
                                predecessors = predecessors.Add(new PredecessorSingle(predecessor.Name, new FactReference(type, hash)));
                                break;
                            case JsonValueKind.Array:
                                ImmutableList<FactReference> factReferences = ImmutableList<FactReference>.Empty;
                                foreach (var factReference in predecessor.Value.EnumerateArray())
                                {
                                    hash = factReference.GetProperty("hash").GetString();
                                    type = factReference.GetProperty("type").GetString();
                                    factReferences = factReferences.Add(new FactReference(type, hash));
                                }
                                predecessors = predecessors.Add(new PredecessorMultiple(predecessor.Name, factReferences));
                                break;
                        }
                    }
                }

                yield return Fact.Create(FactFromDb.name, fields, predecessors);
            }
        }
    }



}
