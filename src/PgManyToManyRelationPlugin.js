// Given a `leftTable`, trace through the foreign key relations
// and identify a `junctionTable` and `rightTable`.
// Returns a list of data objects for these many-to-many relations.
function manyToManyRelations(leftTable, introspectionResultsByKind, omit) {
  return leftTable.foreignConstraints
    .filter(con => con.type === "f" && !omit(con, "read"))
    .reduce((relationInfos, junctionLeftConstraint) => {
      const junctionTable =
        introspectionResultsByKind.classById[junctionLeftConstraint.classId];
      if (!junctionTable) {
        throw new Error(
          `Could not find the table that referenced us (constraint: ${
            junctionLeftConstraint.name
          })`
        );
      }
      const junctionRightConstraint = junctionTable.constraints
        .filter(con => con.type === "f")
        .find(con => con.foreignClassId !== leftTable.id);
      if (!junctionRightConstraint) {
        return relationInfos;
      }
      const rightTable = junctionRightConstraint.foreignClass;

      const leftKeyAttributes = junctionLeftConstraint.foreignKeyAttributes;
      const junctionLeftKeyAttributes = junctionLeftConstraint.keyAttributes;
      const junctionRightKeyAttributes = junctionRightConstraint.keyAttributes;
      const rightKeyAttributes = junctionRightConstraint.foreignKeyAttributes;

      // Ensure keys were found
      if (
        !leftKeyAttributes.every(_ => _) ||
        !junctionLeftKeyAttributes.every(_ => _) ||
        !junctionRightKeyAttributes.every(_ => _) ||
        !rightKeyAttributes.every(_ => _)
      ) {
        throw new Error("Could not find key columns!");
      }

      // Ensure keys can be read
      if (
        leftKeyAttributes.some(attr => omit(attr, "read")) ||
        junctionLeftKeyAttributes.some(attr => omit(attr, "read")) ||
        junctionRightKeyAttributes.some(attr => omit(attr, "read")) ||
        rightKeyAttributes.some(attr => omit(attr, "read"))
      ) {
        return relationInfos;
      }

      // Ensure both constraints are single-column
      // TODO: handle multi-column
      if (leftKeyAttributes.length > 1 || rightKeyAttributes.length > 1) {
        return relationInfos;
      }

      // Ensure junction constraint keys are not unique (which would result in a one-to-one relation)
      const junctionLeftConstraintIsUnique = !!junctionTable.constraints.find(
        c =>
          (c.type === "p" || c.type === "u") &&
          c.keyAttributeNums.length === junctionLeftKeyAttributes.length &&
          c.keyAttributeNums.every(
            (n, i) => junctionLeftKeyAttributes[i].num === n
          )
      );
      const junctionRightConstraintIsUnique = !!junctionTable.constraints.find(
        c =>
          (c.type === "p" || c.type === "u") &&
          c.keyAttributeNums.length === junctionRightKeyAttributes.length &&
          c.keyAttributeNums.every(
            (n, i) => junctionRightKeyAttributes[i].num === n
          )
      );
      if (junctionLeftConstraintIsUnique || junctionRightConstraintIsUnique) {
        return relationInfos;
      }

      relationInfos.push({
        leftKeyAttributes,
        junctionLeftKeyAttributes,
        junctionRightKeyAttributes,
        rightKeyAttributes,
        junctionTable,
        rightTable,
        junctionLeftConstraint,
        junctionRightConstraint,
      });
      return relationInfos;
    }, []);
}

module.exports = function PgManyToManyRelationPlugin(
  builder,
  { pgSimpleCollections }
) {
  builder.hook("inflection", inflection => {
    return Object.assign(inflection, {
      manyToManyRelationByKeys(
        _leftKeyAttributes,
        junctionLeftKeyAttributes,
        junctionRightKeyAttributes,
        _rightKeyAttributes,
        junctionTable,
        rightTable,
        _junctionLeftConstraint,
        junctionRightConstraint
      ) {
        if (junctionRightConstraint.tags.manyToManyFieldName) {
          return junctionRightConstraint.tags.manyToManyFieldName;
        }
        return this.camelCase(
          `${this.pluralize(
            this._singularizedTableName(rightTable)
          )}-by-${this._singularizedTableName(junctionTable)}-${[
            ...junctionLeftKeyAttributes,
            ...junctionRightKeyAttributes,
          ]
            .map(attr => this.column(attr))
            .join("-and-")}`
        );
      },
      manyToManyRelationByKeysSimple(
        _leftKeyAttributes,
        junctionLeftKeyAttributes,
        junctionRightKeyAttributes,
        _rightKeyAttributes,
        junctionTable,
        rightTable,
        _junctionLeftConstraint,
        junctionRightConstraint
      ) {
        if (junctionRightConstraint.tags.manyToManySimpleFieldName) {
          return junctionRightConstraint.tags.manyToManySimpleFieldName;
        }
        return this.camelCase(
          `${this.pluralize(
            this._singularizedTableName(rightTable)
          )}-by-${this._singularizedTableName(junctionTable)}-${[
            ...junctionLeftKeyAttributes,
            ...junctionRightKeyAttributes,
          ]
            .map(attr => this.column(attr))
            .join("-and-")}-list`
        );
      },
    });
  });

  builder.hook(
    "init",
    (_, build) => {
      const {
        newWithHooks,
        pgIntrospectionResultsByKind: introspectionResultsByKind,
        pgGetGqlInputTypeByTypeIdAndModifier,
        graphql: { GraphQLInputObjectType, GraphQLString },
        pgColumnFilter,
        inflection,
        pgOmit: omit,
        describePgEntity,
        sqlCommentByAddingTags,
      } = build;
      introspectionResultsByKind.class.forEach(leftTable => {
        if (!leftTable.isSelectable || omit(leftTable, "filter")) return;
        if (!leftTable.namespace) return;

        const manyToManyRelationsInfo = manyToManyRelations(
          leftTable,
          introspectionResultsByKind,
          omit
        );

        manyToManyRelationsInfo.forEach(
          ({
            leftKeyAttributes,
            junctionLeftKeyAttributes,
            junctionRightKeyAttributes,
            rightKeyAttributes,
            junctionTable,
            rightTable,
            junctionLeftConstraint,
            junctionRightConstraint,
          }) => {
            const relationName = inflection.manyToManyRelationByKeys(
              leftKeyAttributes,
              junctionLeftKeyAttributes,
              junctionRightKeyAttributes,
              rightKeyAttributes,
              junctionTable,
              rightTable,
              junctionLeftConstraint,
              junctionRightConstraint
            );
            newWithHooks(
              GraphQLInputObjectType,
              {
                description: `A condition to be used against \`${relationName}\` object types. All fields are tested for equality and combined with a logical ‘and.’`,
                name: inflection.conditionType(relationName),
                fields: context => {
                  const { fieldWithHooks } = context;
                  return junctionTable.attributes.reduce((memo, attr) => {
                    if (!pgColumnFilter(attr, build, context)) return memo;
                    if (omit(attr, "filter")) return memo;
                    if (junctionLeftKeyAttributes.includes(attr))
                      return memo;
                    if (junctionRightKeyAttributes.includes(attr))
                      return memo;

                    const fieldName = inflection.column(attr);
                    memo = build.extend(
                      memo,
                      {
                        [fieldName]: fieldWithHooks(
                          fieldName,
                          {
                            description: `Checks for equality with the \`${fieldName}\` field in the junction table.`,
                            type:
                              pgGetGqlInputTypeByTypeIdAndModifier(
                                attr.typeId,
                                attr.typeModifier
                              ) || GraphQLString,
                          },
                          {
                            isPgConnectionConditionInputField: true,
                          }
                        ),
                      },
                      `Adding condition argument for ${describePgEntity(attr)}`
                    );
                    return memo;
                  }, {});
                },
              },
              {
                __origin: `Adding condition type for ${describePgEntity(
                  leftTable
                )}. You can rename the table's GraphQL type via:\n\n  ${sqlCommentByAddingTags(
                  leftTable,
                  {
                    name: "newNameHere",
                  }
                )}`,
                pgIntrospection: leftTable,
                isPgCondition: true,
              },
              true // Conditions might all be filtered
            );
          }
        );
      });
      return _;
    },
    ["PgManyToManyRelation"],
    [],
    ["PgTypes"]
  );

  builder.hook("GraphQLObjectType:fields", (fields, build, context) => {
    const {
      extend,
      getTypeByName,
      pgGetGqlTypeByTypeIdAndModifier,
      pgIntrospectionResultsByKind: introspectionResultsByKind,
      pgSql: sql,
      getSafeAliasFromResolveInfo,
      getSafeAliasFromAlias,
      graphql: { GraphQLNonNull, GraphQLList },
      inflection,
      pgQueryFromResolveData: queryFromResolveData,
      pgAddStartEndCursor: addStartEndCursor,
      pgOmit: omit,
      describePgEntity,
    } = build;
    const {
      scope: { isPgRowType, pgIntrospection: leftTable },
      fieldWithHooks,
      Self,
    } = context;
    if (!isPgRowType || !leftTable || leftTable.kind !== "class") {
      return fields;
    }

    const manyToManyRelationsInfo = manyToManyRelations(
      leftTable,
      introspectionResultsByKind,
      omit
    );

    return extend(
      fields,
      manyToManyRelationsInfo.reduce(
        (
          memo,
          {
            leftKeyAttributes,
            junctionLeftKeyAttributes,
            junctionRightKeyAttributes,
            rightKeyAttributes,
            junctionTable,
            rightTable,
            junctionLeftConstraint,
            junctionRightConstraint,
          }
        ) => {
          const RightTableType = pgGetGqlTypeByTypeIdAndModifier(
            rightTable.type.id,
            null
          );
          if (!RightTableType) {
            throw new Error(
              `Could not determine type for table with id ${
                junctionRightConstraint.classId
              }`
            );
          }
          const RightTableConnectionType = getTypeByName(
            inflection.connection(RightTableType.name)
          );

          // Since we're ignoring multi-column keys, we can simplify here
          const leftKeyAttribute = leftKeyAttributes[0];
          const junctionLeftKeyAttribute = junctionLeftKeyAttributes[0];
          const junctionRightKeyAttribute = junctionRightKeyAttributes[0];
          const rightKeyAttribute = rightKeyAttributes[0];

          function makeFields(isConnection) {
            const manyRelationFieldName = isConnection
              ? inflection.manyToManyRelationByKeys(
                  leftKeyAttributes,
                  junctionLeftKeyAttributes,
                  junctionRightKeyAttributes,
                  rightKeyAttributes,
                  junctionTable,
                  rightTable,
                  junctionLeftConstraint,
                  junctionRightConstraint
                )
              : inflection.manyToManyRelationByKeysSimple(
                  leftKeyAttributes,
                  junctionLeftKeyAttributes,
                  junctionRightKeyAttributes,
                  rightKeyAttributes,
                  junctionTable,
                  rightTable,
                  junctionLeftConstraint,
                  junctionRightConstraint
                );

            memo = extend(
              memo,
              {
                [manyRelationFieldName]: fieldWithHooks(
                  manyRelationFieldName,
                  ({
                    getDataFromParsedResolveInfoFragment,
                    addDataGenerator,
                  }) => {
                    addDataGenerator(parsedResolveInfoFragment => {
                      return {
                        pgQuery: queryBuilder => {
                          queryBuilder.select(() => {
                            const resolveData = getDataFromParsedResolveInfoFragment(
                              parsedResolveInfoFragment,
                              isConnection
                                ? RightTableConnectionType
                                : RightTableType
                            );
                            const rightTableAlias = sql.identifier(Symbol());
                            const junctionTableAlias = sql.identifier(Symbol());
                            const leftTableAlias = queryBuilder.getTableAlias();
                            const query = queryFromResolveData(
                              sql.identifier(
                                rightTable.namespace.name,
                                rightTable.name
                              ),
                              rightTableAlias,
                              resolveData,
                              {
                                withPagination: isConnection,
                                withPaginationAsFields: false,
                                asJsonAggregate: !isConnection,
                              },
                              innerQueryBuilder => {
                                innerQueryBuilder.parentQueryBuilder = queryBuilder;
                                innerQueryBuilder.junctionTableAlias = junctionTableAlias;
                                const rightPrimaryKeyConstraint =
                                  rightTable.primaryKeyConstraint;
                                const rightPrimaryKeyAttributes =
                                  rightPrimaryKeyConstraint &&
                                  rightPrimaryKeyConstraint.keyAttributes;
                                if (rightPrimaryKeyAttributes) {
                                  innerQueryBuilder.beforeLock(
                                    "orderBy",
                                    () => {
                                      // append order by primary key to the list of orders
                                      if (
                                        !innerQueryBuilder.isOrderUnique(false)
                                      ) {
                                        innerQueryBuilder.data.cursorPrefix = [
                                          "primary_key_asc",
                                        ];
                                        rightPrimaryKeyAttributes.forEach(
                                          attr => {
                                            innerQueryBuilder.orderBy(
                                              sql.fragment`${innerQueryBuilder.getTableAlias()}.${sql.identifier(
                                                attr.name
                                              )}`,
                                              true
                                            );
                                          }
                                        );
                                        innerQueryBuilder.setOrderIsUnique();
                                      }
                                    }
                                  );
                                }

                                // I would use `innerQueryBuilder.join()` if that existed,
                                // but it doesn't, so I have to reach into `data`. :-(
                                innerQueryBuilder.data.join.push(
                                  sql.fragment`INNER JOIN ${sql.identifier(
                                    junctionTable.namespace.name,
                                    junctionTable.name
                                  )} AS ${
                                    junctionTableAlias
                                  } ON (${rightTableAlias}.${sql.identifier(
                                    rightKeyAttribute.name
                                  )} = ${junctionTableAlias}.${sql.identifier(
                                    junctionRightKeyAttribute.name
                                  )})`
                                );
                                innerQueryBuilder.data.join.push(
                                  sql.fragment`INNER JOIN ${sql.identifier(
                                    leftTable.namespace.name,
                                    leftTable.name
                                  )} AS ${
                                    leftTableAlias
                                  } ON (${leftTableAlias}.${sql.identifier(
                                    leftKeyAttribute.name
                                  )} = ${junctionTableAlias}.${sql.identifier(
                                    junctionLeftKeyAttribute.name
                                  )})`
                                );
                              }
                            );
                            return sql.fragment`(${query})`;
                          }, getSafeAliasFromAlias(parsedResolveInfoFragment.alias));
                        },
                      };
                    });

                    const rightTableTypeName = inflection.tableType(rightTable);
                    return {
                      description: `Reads and enables pagination through a set of \`${rightTableTypeName}\`.`,
                      type: isConnection
                        ? new GraphQLNonNull(RightTableConnectionType)
                        : new GraphQLNonNull(
                            new GraphQLList(new GraphQLNonNull(RightTableType))
                          ),
                      args: {},
                      resolve: (data, _args, _context, resolveInfo) => {
                        const safeAlias = getSafeAliasFromResolveInfo(
                          resolveInfo
                        );
                        if (isConnection) {
                          return addStartEndCursor(data[safeAlias]);
                        } else {
                          return data[safeAlias];
                        }
                      },
                    };
                  },
                  {
                    isPgFieldConnection: isConnection,
                    isPgFieldSimpleCollection: !isConnection,
                    isPgManyToManyRelationField: true,
                    pgFieldIntrospection: rightTable,

                    pgManyToManyLeftTable: leftTable,
                    pgManyToManyLeftKeyAttributes: leftKeyAttributes,
                    pgManyToManyRightTable: rightTable,
                    pgManyToManyRightKeyAttributes: rightKeyAttributes,
                    pgManyToManyJunctionTable: junctionTable,
                    pgManyToManyJunctionLeftConstraint: junctionLeftConstraint,
                    pgManyToManyJunctionRightConstraint: junctionRightConstraint,
                    pgManyToManyJunctionLeftKeyAttributes: junctionLeftKeyAttributes,
                    pgManyToManyJunctionRightKeyAttributes: junctionRightKeyAttributes,
                  }
                ),
              },

              `Many-to-many relation (${
                isConnection ? "connection" : "simple collection"
              }) for ${describePgEntity(
                junctionLeftConstraint
              )} and ${describePgEntity(junctionRightConstraint)}.`
            );
          }

          const simpleCollections =
            junctionRightConstraint.tags.simpleCollections ||
            rightTable.tags.simpleCollections ||
            pgSimpleCollections;
          const hasConnections = simpleCollections !== "only";
          const hasSimpleCollections =
            simpleCollections === "only" || simpleCollections === "both";
          if (hasConnections) {
            makeFields(true);
          }
          if (hasSimpleCollections) {
            makeFields(false);
          }
          return memo;
        },
        {}
      ),
      `Adding many-to-many relations for ${Self.name}`
    );
  });

  builder.hook(
    "GraphQLObjectType:fields:field:args",
    (args, build, context) => {
      const {
        pgSql: sql,
        gql2pg,
        extend,
        getTypeByName,
        pgColumnFilter,
        inflection,
        pgOmit: omit,
      } = build;
      const {
        scope: {
          fieldName,
          isPgManyToManyRelationField,
          isPgFieldConnection,
          isPgFieldSimpleCollection,

          pgManyToManyLeftKeyAttributes: leftKeyAttributes,
          pgManyToManyRightTable: rightTable,
          pgManyToManyRightKeyAttributes: rightKeyAttributes,
          pgManyToManyJunctionTable: junctionTable,
          pgManyToManyJunctionLeftConstraint: junctionLeftConstraint,
          pgManyToManyJunctionRightConstraint: junctionRightConstraint,
          pgManyToManyJunctionLeftKeyAttributes: junctionLeftKeyAttributes,
          pgManyToManyJunctionRightKeyAttributes: junctionRightKeyAttributes,
        },
        addArgDataGenerator,
        Self,
        field,
      } = context;

      if (!isPgManyToManyRelationField) return args;

      const shouldAddCondition =
        isPgFieldConnection || isPgFieldSimpleCollection;
      if (!shouldAddCondition) return args;

      const TableConditionType = getTypeByName(
        inflection.conditionType(
          inflection.manyToManyRelationByKeys(
            leftKeyAttributes,
            junctionLeftKeyAttributes,
            junctionRightKeyAttributes,
            rightKeyAttributes,
            junctionTable,
            rightTable,
            junctionLeftConstraint,
            junctionRightConstraint,
          )
        )
      );
      if (!TableConditionType) {
        return args;
      }

      const relevantAttributes = junctionTable.attributes.filter(
        attr =>
          pgColumnFilter(attr, build, context) &&
          !omit(attr, "filter") &&
          !junctionLeftKeyAttributes.includes(attr) &&
          !junctionRightKeyAttributes.includes(attr)
      );

      addArgDataGenerator(function connectionCondition({ condition }) {
        return {
          pgQuery: queryBuilder => {
            if (condition != null) {
              // This is a bit precarious, and it doesn't yet work for live queries...
              const junctionTableAlias = queryBuilder.junctionTableAlias;
              if (!junctionTableAlias) {
                throw new Error('Missing junctionTableAlias on queryBuilder');
              }
              relevantAttributes.forEach(attr => {
                const fieldName = inflection.column(attr);
                const val = condition[fieldName];
                if (val != null) {
                  // queryBuilder.addLiveCondition(() => record =>
                  //   record[attr.name] === val
                  // );
                  queryBuilder.where(
                    sql.fragment`${junctionTableAlias}.${sql.identifier(
                      attr.name
                    )} = ${gql2pg(val, attr.type, attr.typeModifier)}`
                  );
                } else if (val === null) {
                  // queryBuilder.addLiveCondition(() => record =>
                  //   record[attr.name] == null
                  // );
                  queryBuilder.where(
                    sql.fragment`${junctionTableAlias}.${sql.identifier(
                      attr.name
                    )} IS NULL`
                  );
                }
              });
            }
          },
        };
      });

      // The `PgConnectionArgCondition` plugin adds a `condition` argument,
      // but it doesn't do what we want. So we'll delete that argument,
      // and set our own instead.
      delete args.condition;

      return extend(
        args,
        {
          condition: {
            description:
              "A condition to be used in determining which values should be returned by the collection.",
            type: TableConditionType,
          },
        },
        `Adding condition to connection field '${fieldName ||
          field.type}' of '${Self.name}'`
      );
    },
    ["PgManyToManyRelation"]
  );
};
