from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
import time
from sql import SQLTableManager
from tabla import TableStorageManager

class SQLRequest(BaseModel):
    sql: str

app = FastAPI(title="SQL Database", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

sql_manager = None

@app.on_event("startup")
async def startup():
    """Inicializa el sistema al arrancar"""
    global sql_manager
    os.makedirs('tablas', exist_ok=True)
    os.makedirs('indices', exist_ok=True)
    sql_manager = SQLTableManager(storage_class=TableStorageManager, base_dir='tablas')
    print("🚀 Sistema SQL inicializado")

@app.get("/")
async def root():
    """Info básica de la API"""
    return {"message": "🗄️ SQL Database API", "status": "active", "docs": "/docs"}

@app.post("/sql")
async def execute_sql(request: SQLRequest):
   
    
    if not sql_manager:
        raise HTTPException(status_code=500, detail="Sistema no inicializado")
    
    try:
        start_time = time.time()
        
        operations = sql_manager.execute_sql(request.sql)
        
        execution_time = time.time() - start_time
        
        result = {
            "sql": request.sql,
            "execution_time": round(execution_time, 4),
            "success": True,
            "results": []
        }
        
        for op_type, op_result in operations:
            
            if op_type == "CREATE":
                result["results"].append({
                    "operation": "CREATE",
                    "table_created": op_result,
                    "message": f"Tabla '{op_result}' creada exitosamente"
                })
            
            elif op_type == "INSERT":
                if isinstance(op_result, dict) and 'records' in op_result:
                    inserted_count = len(op_result['records'])
                    result["results"].append({
                        "operation": "INSERT",
                        "records_inserted": inserted_count,
                        "inserted_ids": op_result.get('inserted_ids', []),
                        "message": f"{inserted_count} registro(s) insertado(s)"
                    })
            
            elif op_type == "SELECT":
                if not op_result.get('error', False):
                    resultado = op_result.get('resultado', {})
                    if not resultado.get('error', False):
                        found_records = resultado.get('numeros_registro', [])
                        table_name = op_result.get('table_name', '')
                        requested_attributes = resultado.get('requested_attributes', [])
                        
                        records_data = []
                        if found_records and table_name:
                            storage_manager = sql_manager.get_storage_manager(table_name)
                            if storage_manager:
                                for record_num in found_records:
                                    record = storage_manager.get(record_num)
                                    if record:
                                        if requested_attributes:
                                            filtered_record = {k: v for k, v in record.items() if k in requested_attributes}
                                            records_data.append({"record_id": record_num, **filtered_record})
                                        else:
                                            records_data.append({"record_id": record_num, **record})
                        
                        result["results"].append({
                            "operation": "SELECT",
                            "table": table_name,
                            "records_found": len(found_records),
                            "records": records_data,
                            "message": f"Se encontraron {len(found_records)} registro(s)"
                        })
                    else:
                        result["results"].append({
                            "operation": "SELECT",
                            "error": True,
                            "message": resultado.get('message', 'Error en SELECT')
                        })
                else:
                    result["results"].append({
                        "operation": "SELECT",
                        "error": True,
                        "message": op_result.get('message', 'Error en SELECT')
                    })
            
            elif op_type == "DELETE":
                if not op_result.get('error', False):
                    deleted_count = op_result.get('count', 0)
                    result["results"].append({
                        "operation": "DELETE",
                        "records_deleted": deleted_count,
                        "deleted_ids": op_result.get('records_deleted', []),
                        "message": f"{deleted_count} registro(s) eliminado(s)"
                    })
                else:
                    result["results"].append({
                        "operation": "DELETE",
                        "error": True,
                        "message": op_result.get('message', 'Error en DELETE')
                    })
            
            elif op_type == "IMPORT_CSV":
                if not op_result.get('error', False):
                    successful = op_result.get('successful_inserts', 0)
                    failed = op_result.get('failed_inserts_count', 0)
                    csv_file = op_result.get('csv_file', '')
                    result["results"].append({
                        "operation": "IMPORT_CSV",
                        "csv_file": os.path.basename(csv_file),
                        "records_imported": successful,
                        "records_failed": failed,
                        "message": f"Importados {successful} registros desde CSV"
                    })
                else:
                    result["results"].append({
                        "operation": "IMPORT_CSV",
                        "error": True,
                        "message": op_result.get('message', 'Error en importación CSV')
                    })
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error SQL: {str(e)}")

@app.get("/tables")
async def get_tables():
    """
    Obtiene la lista de todas las tablas disponibles (versión simplificada)
    """
    if not sql_manager:
        raise HTTPException(status_code=500, detail="Sistema no inicializado")
    
    try:
        import os
        tables_info = {}
        
        if os.path.exists('tablas'):
            for filename in os.listdir('tablas'):
                if filename.endswith('.bin') or filename.endswith('.dat') or filename.endswith('.txt'):
                    table_name = filename.split('.')[0] 
                    try:
                        storage_manager = sql_manager.get_storage_manager(table_name)
                        if storage_manager and hasattr(storage_manager, 'records'):
                            record_count = len(storage_manager.records)
                        else:
                            record_count = 0
                    except:
                        record_count = 0
                    
                    tables_info[table_name] = {
                        "record_count": record_count,
                        "primary_key": "",
                        "columns": [],
                        "indexes": []
                    }
        
        return {
            "success": True,
            "tables": tables_info,
            "total_tables": len(tables_info)
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": f"Error obteniendo tablas: {str(e)}",
            "tables": {},
            "total_tables": 0
        }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)