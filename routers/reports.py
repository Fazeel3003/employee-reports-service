# routers/reports.py - start with simple endpoint
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from database import get_connection
import pandas as pd
import io
import logging
from logging.handlers import RotatingFileHandler
import os

router = APIRouter()

# Configure logging with file rotation
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(
            os.path.join(log_dir, 'reports_service.log'),
            maxBytes=10485760,
            backupCount=5
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@router.get(
    "/employees/export",
    summary="Export Employees Data",
    description="Export all employees data in CSV or JSON format with department and position information",
    responses={
        200: {
            "description": "Successful export",
            "content": {
                "text/csv": {"example": "employee_code,first_name,last_name,email,..."},
                "application/json": {
                    "example": {
                        "success": True,
                        "data": [{"employee_code": "EMP001", "first_name": "John"}],
                        "metadata": {"total_records": 1, "format": "json"}
                    }
                }
            }
        },
        500: {"description": "Database connection failed or internal error"}
    }
)
async def export_employees(format: str = "csv"):
    """Export all employees as CSV or JSON file"""
    logger.info(f"Employee export requested - format: {format}")
    
    try:
        conn = get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
            
        cursor = conn.cursor(dictionary=True)
        
        # Get employee data with related info
        cursor.execute("""
            SELECT 
                e.employee_code,
                e.first_name,
                e.last_name,
                e.email,
                e.phone,
                e.hire_date,
                e.status,
                d.dept_name as department,
                p.position_title as position
            FROM employees e
            LEFT JOIN departments d ON e.dept_id = d.dept_id
            LEFT JOIN positions p ON e.position_id = p.position_id
            ORDER BY e.emp_id
        """)
        
        employees = cursor.fetchall()
        cursor.close()
        conn.close()
        
        if format.lower() == "json":
            # Return JSON response
            import json
            return {
                "success": True,
                "data": employees,
                "metadata": {
                    "total_records": len(employees),
                    "generated_at": pd.Timestamp.now().isoformat(),
                    "format": "json"
                }
            }
        else:
            # CSV export
            df = pd.DataFrame(employees)
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            logger.info(f"CSV export generated: {len(employees)} employees")
            
            return StreamingResponse(
                io.BytesIO(csv_buffer.getvalue().encode()),
                media_type="text/csv",
                headers={
                    "Content-Disposition": "attachment; filename=employees_export.csv"
                }
            )
            
    except Exception as e:
        logger.error(f"Error exporting employees: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get(
    "/departments/summary",
    summary="Department Summary Report",
    description="Get summary statistics for all departments including employee count and active percentage",
    responses={
        200: {
            "description": "Successful report generation",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": [
                            {"dept_name": "Engineering", "employee_count": 25, "active_percentage": 96.0}
                        ],
                        "metadata": {"total_departments": 5}
                    }
                }
            }
        },
        500: {"description": "Database connection failed or internal error"}
    }
)
async def department_summary():
    """Get department summary report"""
    logger.info("Department summary requested")
    
    try:
        conn = get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
            
        cursor = conn.cursor(dictionary=True)
        
        # Get department counts
        cursor.execute("""
            SELECT 
                d.dept_name,
                COUNT(e.emp_id) as employee_count,
                AVG(CASE WHEN e.status = 'Active' THEN 1 ELSE 0 END) * 100 as active_percentage
            FROM departments d
            LEFT JOIN employees e ON d.dept_id = e.dept_id
            GROUP BY d.dept_name
            ORDER BY employee_count DESC
        """)
        
        departments = cursor.fetchall()
        cursor.close()
        conn.close()
        
        logger.info(f"Department summary generated: {len(departments)} departments")
        
        return {
            "success": True,
            "data": departments,
            "metadata": {
                "total_departments": len(departments),
                "generated_at": pd.Timestamp.now().isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"Error generating department summary: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get(
    "/attendance/summary",
    summary="Attendance Summary Report",
    description="Get attendance summary with optional date range filtering. Shows present, absent, and half-day counts by date and department",
    responses={
        200: {
            "description": "Successful report generation",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": [
                            {
                                "date": "2024-01-15",
                                "total_records": 50,
                                "present": 45,
                                "absent": 3,
                                "half_day": 2,
                                "department": "Engineering"
                            }
                        ],
                        "metadata": {
                            "total_records": 100,
                            "date_range": {"start": "2024-01-01", "end": "2024-01-31"}
                        }
                    }
                }
            }
        },
        500: {"description": "Database connection failed or internal error"}
    }
)
async def attendance_summary(start_date: str = None, end_date: str = None):
    """Get attendance summary report"""
    logger.info(f"Attendance summary requested - start: {start_date}, end: {end_date}")
    
    try:
        conn = get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
            
        cursor = conn.cursor(dictionary=True)
        
        # Build query with date filters
        query = """
            SELECT 
                DATE(a.attendance_date) as date,
                COUNT(*) as total_records,
                SUM(CASE WHEN a.attendance_status = 'Present' THEN 1 ELSE 0 END) as present,
                SUM(CASE WHEN a.attendance_status = 'Absent' THEN 1 ELSE 0 END) as absent,
                SUM(CASE WHEN a.attendance_status = 'Half Day' THEN 1 ELSE 0 END) as half_day,
                d.dept_name as department
            FROM attendance a
            LEFT JOIN employees e ON a.emp_id = e.emp_id
            LEFT JOIN departments d ON e.dept_id = d.dept_id
        """
        
        params = []
        if start_date and end_date:
            query += " WHERE a.attendance_date BETWEEN %s AND %s"
            params = [start_date, end_date]
        elif start_date:
            query += " WHERE a.attendance_date >= %s"
            params = [start_date]
        elif end_date:
            query += " WHERE a.attendance_date <= %s"
            params = [end_date]
            
        query += " GROUP BY DATE(a.attendance_date), d.dept_name ORDER BY date DESC"
        
        cursor.execute(query, params)
        attendance_data = cursor.fetchall()
        cursor.close()
        conn.close()
        
        logger.info(f"Attendance summary generated: {len(attendance_data)} records")
        
        return {
            "success": True,
            "data": attendance_data,
            "metadata": {
                "total_records": len(attendance_data),
                "date_range": {"start": start_date, "end": end_date},
                "generated_at": pd.Timestamp.now().isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"Error generating attendance summary: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get(
    "/attendance/export",
    summary="Export Attendance as CSV",
    description="Export filtered attendance summary as a CSV file based on optional date range",
    responses={
        200: {"description": "CSV file download", "content": {"text/csv": {}}},
        500: {"description": "Database connection failed or internal error"}
    }
)
async def export_attendance_csv(start_date: str = None, end_date: str = None):
    """Export attendance summary as CSV with optional date range filter"""
    logger.info(f"Attendance CSV export requested - start: {start_date}, end: {end_date}")

    try:
        conn = get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")

        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT
                DATE(a.attendance_date) as date,
                d.dept_name as department,
                COUNT(*) as total_records,
                SUM(CASE WHEN a.attendance_status = 'Present' THEN 1 ELSE 0 END) as present,
                SUM(CASE WHEN a.attendance_status = 'Absent' THEN 1 ELSE 0 END) as absent,
                SUM(CASE WHEN a.attendance_status = 'Half Day' THEN 1 ELSE 0 END) as half_day
            FROM attendance a
            LEFT JOIN employees e ON a.emp_id = e.emp_id
            LEFT JOIN departments d ON e.dept_id = d.dept_id
        """

        params = []
        if start_date and end_date:
            query += " WHERE a.attendance_date BETWEEN %s AND %s"
            params = [start_date, end_date]
        elif start_date:
            query += " WHERE a.attendance_date >= %s"
            params = [start_date]
        elif end_date:
            query += " WHERE a.attendance_date <= %s"
            params = [end_date]

        query += " GROUP BY DATE(a.attendance_date), d.dept_name ORDER BY date DESC"

        cursor.execute(query, params)
        attendance_data = cursor.fetchall()
        cursor.close()
        conn.close()

        df = pd.DataFrame(attendance_data)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        date_suffix = f"{start_date}_to_{end_date}" if start_date and end_date else "all"
        filename = f"attendance_report_{date_suffix}.csv"

        logger.info(f"Attendance CSV export generated: {len(attendance_data)} records")

        return StreamingResponse(
            io.BytesIO(csv_buffer.getvalue().encode()),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except Exception as e:
        logger.error(f"Error exporting attendance CSV: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/test",
    summary="Test Endpoint",
    description="Simple health check endpoint to verify the Reports API is running",
    responses={
        200: {
            "description": "API is healthy",
            "content": {
                "application/json": {
                    "example": {"message": "Reports API is working", "status": "healthy"}
                }
            }
        }
    }
)
def test_endpoint():
    """Test endpoint for connectivity"""
    return {"message": "Reports API is working", "status": "healthy"}

@router.get(
    "/employees/export-excel",
    summary="Export Employees to Excel",
    description="Export all employees data as an Excel (.xlsx) file with formatting",
    responses={
        200: {
            "description": "Excel file download",
            "content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}}
        },
        500: {"description": "Database connection failed or internal error"}
    }
)
async def export_employees_excel():
    """Export all employees as Excel file"""
    logger.info("Employee Excel export requested")
    
    try:
        conn = get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
            
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT 
                e.employee_code,
                e.first_name,
                e.last_name,
                e.email,
                e.phone,
                e.hire_date,
                e.status,
                d.dept_name as department,
                p.position_title as position
            FROM employees e
            LEFT JOIN departments d ON e.dept_id = d.dept_id
            LEFT JOIN positions p ON e.position_id = p.position_id
            ORDER BY e.emp_id
        """)
        
        employees = cursor.fetchall()
        cursor.close()
        conn.close()
        
        df = pd.DataFrame(employees)
        excel_buffer = io.BytesIO()
        
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Employees', index=False)
        
        excel_buffer.seek(0)
        
        logger.info(f"Excel export generated: {len(employees)} employees")
        
        return StreamingResponse(
            excel_buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=employees_export.xlsx"
            }
        )
        
    except Exception as e:
        logger.error(f"Error exporting employees to Excel: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))