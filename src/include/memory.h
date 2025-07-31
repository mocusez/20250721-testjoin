#ifndef MEMORY_H
#define MEMORY_H

#include <stdint.h>

// Schema description for each column (field name, type, nullable)
typedef struct {
  const char *name; // Column name
  int32_t type_id;  // Type identifier (consistent with NDBArrayC)
  int8_t nullable;  // Whether null values are allowed
} NDBFieldC;

// Data for each column (i.e., NDB Array)
typedef struct {
  uint8_t *validity; // Null bitmap (used only for nullable fields)
  int32_t *offsets;  // Used for variable-length types, e.g., string/list
  void *values;      // Actual value buffer, e.g., int32_t*, float*, char*
  int32_t length;
  int32_t null_count;
  int32_t type_id; // Type identifier, e.g., 0=int32, 1=string
} NDBArrayC;

// Table structure
typedef struct {
  NDBFieldC *fields;  // Schema metadata for each column
  NDBArrayC *columns; // Actual column data
  int32_t num_columns;
  int32_t num_rows;
} NDBTableC;

#endif // MEMORY_H