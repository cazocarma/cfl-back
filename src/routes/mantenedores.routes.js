const express = require("express");
const { getPool } = require("../db");
const { MAINTAINERS } = require("../mantenedores-config");

const router = express.Router();

function getEntityConfig(entity) {
  return MAINTAINERS[entity] || null;
}

function toBool(value) {
  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "number") {
    return value !== 0;
  }

  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    return ["1", "true", "t", "yes", "si", "y"].includes(normalized);
  }

  return false;
}

function normalizeValue(fieldName, value) {
  if (value === undefined) {
    return value;
  }

  const lower = fieldName.toLowerCase();
  const isBooleanField = lower.startsWith("activo") || lower.startsWith("activa") || lower.startsWith("cerrada") || lower.includes("requiere_");
  if (isBooleanField) {
    return toBool(value);
  }

  return value;
}

function collectPayload(body, allowedFields) {
  const payload = {};

  for (const fieldName of allowedFields) {
    if (Object.prototype.hasOwnProperty.call(body, fieldName)) {
      payload[fieldName] = normalizeValue(fieldName, body[fieldName]);
    }
  }

  return payload;
}

function requireEntity(req, res) {
  const entity = req.params.entity;
  const entityConfig = getEntityConfig(entity);

  if (!entityConfig) {
    res.status(404).json({ error: `Mantenedor no soportado: ${entity}` });
    return null;
  }

  return entityConfig;
}

function parseEntityId(req, res, entityConfig) {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    res.status(400).json({ error: `ID invalido para ${entityConfig.title}` });
    return null;
  }

  return id;
}

function buildBaseFrom(entityConfig) {
  if (entityConfig.from) {
    return entityConfig.from;
  }

  return `${entityConfig.table} ${entityConfig.alias}`;
}

async function fetchEntityById(pool, entityConfig, id) {
  const sql = `
    SELECT ${entityConfig.listColumns.join(", ")}
    FROM ${buildBaseFrom(entityConfig)}
    WHERE ${entityConfig.alias}.${entityConfig.idColumn} = @id;
  `;

  const result = await pool.request().input("id", id).query(sql);
  return result.recordset[0] || null;
}

router.get("/resumen", async (req, res, next) => {
  try {
    const pool = await getPool();
    const summary = [];

    for (const [key, entityConfig] of Object.entries(MAINTAINERS)) {
      const countSql = `SELECT COUNT_BIG(1) AS total FROM ${entityConfig.table};`;
      const countResult = await pool.request().query(countSql);
      summary.push({
        key,
        title: entityConfig.title,
        total: Number(countResult.recordset[0].total),
      });
    }

    res.json({
      data: summary,
      generated_at: new Date().toISOString(),
    });
  } catch (error) {
    next(error);
  }
});

router.get("/:entity", async (req, res, next) => {
  const entityConfig = requireEntity(req, res);
  if (!entityConfig) {
    return;
  }

  try {
    const pool = await getPool();
    const sql = `
      SELECT ${entityConfig.listColumns.join(", ")}
      FROM ${buildBaseFrom(entityConfig)}
      ORDER BY ${entityConfig.orderBy};
    `;

    const result = await pool.request().query(sql);
    res.json({
      data: result.recordset,
      total: result.recordset.length,
    });
  } catch (error) {
    next(error);
  }
});

router.post("/:entity", async (req, res, next) => {
  const entityConfig = requireEntity(req, res);
  if (!entityConfig) {
    return;
  }

  try {
    const allowedFields = [
      ...entityConfig.create.required,
      ...entityConfig.create.optional,
    ];
    const payload = collectPayload(req.body || {}, allowedFields);

    const missingRequired = entityConfig.create.required.filter(
      (fieldName) =>
        payload[fieldName] === undefined ||
        payload[fieldName] === null ||
        payload[fieldName] === ""
    );

    if (missingRequired.length > 0) {
      res.status(400).json({
        error: "Faltan campos requeridos",
        missing_fields: missingRequired,
      });
      return;
    }

    if (entityConfig.timestamps?.created) {
      payload[entityConfig.timestamps.created] = new Date();
    }
    if (entityConfig.timestamps?.updated) {
      payload[entityConfig.timestamps.updated] = new Date();
    }

    const fields = Object.keys(payload);
    const request = (await getPool()).request();

    fields.forEach((fieldName, index) => {
      request.input(`p${index}`, payload[fieldName]);
    });

    const insertSql = `
      INSERT INTO ${entityConfig.table} (${fields.map((field) => `[${field}]`).join(", ")})
      OUTPUT INSERTED.[${entityConfig.idColumn}] AS id
      VALUES (${fields.map((_, index) => `@p${index}`).join(", ")});
    `;

    const insertResult = await request.query(insertSql);
    const insertedId = insertResult.recordset[0].id;
    const insertedRow = await fetchEntityById(await getPool(), entityConfig, insertedId);

    res.status(201).json({
      message: `${entityConfig.title} creado`,
      data: insertedRow || { [entityConfig.idColumn]: insertedId },
    });
  } catch (error) {
    next(error);
  }
});

router.put("/:entity/:id", async (req, res, next) => {
  const entityConfig = requireEntity(req, res);
  if (!entityConfig) {
    return;
  }

  const id = parseEntityId(req, res, entityConfig);
  if (id === null) {
    return;
  }

  try {
    const payload = collectPayload(req.body || {}, entityConfig.update.allowed);

    if (entityConfig.timestamps?.updated) {
      payload[entityConfig.timestamps.updated] = new Date();
    }

    const fields = Object.keys(payload);
    if (fields.length === 0) {
      res.status(400).json({
        error: "No se recibieron campos para actualizar",
      });
      return;
    }

    const request = (await getPool()).request();
    request.input("id", id);

    const setClause = fields
      .map((fieldName, index) => {
        request.input(`p${index}`, payload[fieldName]);
        return `[${fieldName}] = @p${index}`;
      })
      .join(", ");

    const updateSql = `
      UPDATE ${entityConfig.table}
      SET ${setClause}
      WHERE [${entityConfig.idColumn}] = @id;
    `;

    const updateResult = await request.query(updateSql);
    if (updateResult.rowsAffected[0] === 0) {
      res.status(404).json({ error: `${entityConfig.title} no encontrado` });
      return;
    }

    const updatedRow = await fetchEntityById(await getPool(), entityConfig, id);
    res.json({
      message: `${entityConfig.title} actualizado`,
      data: updatedRow,
    });
  } catch (error) {
    next(error);
  }
});

router.delete("/:entity/:id", async (req, res, next) => {
  const entityConfig = requireEntity(req, res);
  if (!entityConfig) {
    return;
  }

  const id = parseEntityId(req, res, entityConfig);
  if (id === null) {
    return;
  }

  try {
    if (entityConfig.softDeleteColumn) {
      const request = (await getPool()).request();
      request.input("id", id);
      request.input("active", false);

      let setClause = `[${entityConfig.softDeleteColumn}] = @active`;
      if (entityConfig.timestamps?.updated) {
        request.input("updatedAt", new Date());
        setClause += `, [${entityConfig.timestamps.updated}] = @updatedAt`;
      }

      const sql = `
        UPDATE ${entityConfig.table}
        SET ${setClause}
        WHERE [${entityConfig.idColumn}] = @id;
      `;

      const result = await request.query(sql);
      if (result.rowsAffected[0] === 0) {
        res.status(404).json({ error: `${entityConfig.title} no encontrado` });
        return;
      }
    } else {
      const sql = `
        DELETE FROM ${entityConfig.table}
        WHERE [${entityConfig.idColumn}] = @id;
      `;
      const result = await (await getPool()).request().input("id", id).query(sql);

      if (result.rowsAffected[0] === 0) {
        res.status(404).json({ error: `${entityConfig.title} no encontrado` });
        return;
      }
    }

    res.json({
      message: `${entityConfig.title} eliminado`,
      id,
    });
  } catch (error) {
    next(error);
  }
});

module.exports = {
  mantenedoresRouter: router,
};
