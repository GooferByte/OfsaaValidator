# src/report_generator.py

import json
from typing import Dict, List
from datetime import datetime
from pathlib import Path
import pandas as pd

class ReportGenerator:
    """Generate comprehensive validation reports"""
    
    def generate_report(self,
                       valid_df: pd.DataFrame,
                       rejected_df: pd.DataFrame,
                       errors: List,
                       summary: Dict,
                       parse_metadata: Dict,
                       output_dir: str):
        """Generate all report files"""
        
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        report_data = {
            'summary': summary,
            'parse_metadata': parse_metadata,
            'error_analysis': self._analyze_errors(errors),
            'recommendations': self._generate_recommendations(summary, errors, rejected_df),
            'top_errors': [e.to_dict() for e in errors[:50]],  # Top 50 errors
            'timestamp': datetime.now().isoformat()
        }
        
        # 1. JSON Report
        json_path = f"{output_dir}/validation_report.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)
        print(f"  ✓ JSON report: {json_path}")
        
        # 2. HTML Report
        html_path = f"{output_dir}/validation_report.html"
        self._generate_html_report(report_data, html_path)
        print(f"  ✓ HTML report: {html_path}")
        
        # 3. Excel Report (with multiple sheets)
        excel_path = f"{output_dir}/validation_report.xlsx"
        self._generate_excel_report(valid_df, rejected_df, errors, summary, excel_path)
        print(f"  ✓ Excel report: {excel_path}")
        
        # 4. Fix Instructions
        fix_path = f"{output_dir}/fix_instructions.txt"
        self._generate_fix_instructions(errors, fix_path)
        print(f"  ✓ Fix instructions: {fix_path}")
        
        return report_data
    
    def _analyze_errors(self, errors: List) -> Dict:
        """Analyze error patterns"""
        
        error_by_type = {}
        error_by_column = {}
        error_by_row = {}
        
        for error in errors:
            # By type
            error_by_type[error.error_type] = error_by_type.get(error.error_type, 0) + 1
            
            # By column
            error_by_column[error.column_name] = error_by_column.get(error.column_name, 0) + 1
            
            # By row
            error_by_row[error.row_number] = error_by_row.get(error.row_number, 0) + 1
        
        # Find rows with most errors
        worst_rows = sorted(error_by_row.items(), key=lambda x: x[1], reverse=True)[:10]
        
        return {
            'total_errors': len(errors),
            'error_by_type': dict(sorted(error_by_type.items(), key=lambda x: x[1], reverse=True)),
            'error_by_column': dict(sorted(error_by_column.items(), key=lambda x: x[1], reverse=True)),
            'worst_rows': [{'row': row, 'error_count': count} for row, count in worst_rows]
        }
    
    def _generate_recommendations(self, summary: Dict, errors: List, rejected_df: pd.DataFrame) -> List[str]:
        """Generate actionable recommendations"""
        
        recommendations = []
        
        # Data quality assessment
        quality = summary['data_quality_score']
        if quality < 70:
            recommendations.append({
                'severity': 'CRITICAL',
                'message': f'Data quality is {quality}% - CRITICAL issues detected',
                'action': f'Fix {summary["rejected_records"]} rejected records before OFSAA load',
                'priority': 1
            })
        elif quality < 90:
            recommendations.append({
                'severity': 'WARNING',
                'message': f'Data quality is {quality}% - Significant issues found',
                'action': 'Review and fix rejected records',
                'priority': 2
            })
        elif quality < 95:
            recommendations.append({
                'severity': 'INFO',
                'message': f'Data quality is {quality}% - Minor issues detected',
                'action': 'Review rejected records before load',
                'priority': 3
            })
        else:
            recommendations.append({
                'severity': 'SUCCESS',
                'message': f'Excellent data quality ({quality}%)',
                'action': 'Ready for OFSAA load',
                'priority': 0
            })
        
        # Error-specific recommendations
        if errors:
            # Most common error type
            error_types = {}
            for error in errors:
                error_types[error.error_type] = error_types.get(error.error_type, 0) + 1
            
            top_error = max(error_types.items(), key=lambda x: x[1])
            error_type, count = top_error
            
            if error_type == 'VALUE_MISSING':
                recommendations.append({
                    'severity': 'HIGH',
                    'message': f'{count} mandatory field violations',
                    'action': 'Populate all required fields with valid data',
                    'priority': 1
                })
            elif error_type == 'INVALID_DATA_TYPE':
                recommendations.append({
                    'severity': 'HIGH',
                    'message': f'{count} data type mismatches',
                    'action': 'Verify data formats match OFSAA requirements (dates as YYYYMMDD, numbers without text)',
                    'priority': 1
                })
            elif error_type == 'LENGTH_EXCEEDED':
                recommendations.append({
                    'severity': 'MEDIUM',
                    'message': f'{count} length violations',
                    'action': 'Truncate or split long values to meet field length requirements',
                    'priority': 2
                })
            
            # Column-specific recommendations
            column_errors = {}
            for error in errors:
                column_errors[error.column_name] = column_errors.get(error.column_name, 0) + 1
            
            top_column = max(column_errors.items(), key=lambda x: x[1])
            col_name, count = top_column
            
            recommendations.append({
                'severity': 'INFO',
                'message': f'Column "{col_name}" has {count} errors',
                'action': f'Focus on fixing "{col_name}" first - it has the most issues',
                'priority': 2
            })
        
        # Sort by priority
        recommendations.sort(key=lambda x: x['priority'])
        
        return recommendations
    
    def _generate_html_report(self, report_data: Dict, output_path: str):
        """Generate HTML report"""
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>OFSAA Validation Report<title>OFSAA Validation Report</title>
    <meta charset="UTF-8">
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #34495e;
            margin-top: 30px;
            border-left: 4px solid #3498db;
            padding-left: 10px;
        }}
        .summary-box {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .metric {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
        }}
        .metric.success {{
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        }}
        .metric.warning {{
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        }}
        .metric-value {{
            font-size: 32px;
            font-weight: bold;
            margin: 10px 0;
        }}
        .metric-label {{
            font-size: 14px;
            opacity: 0.9;
        }}
        .quality-score {{
            font-size: 48px;
            font-weight: bold;
            text-align: center;
            margin: 20px 0;
            padding: 30px;
            border-radius: 8px;
        }}
        .quality-excellent {{ background: #d4edda; color: #155724; }}
        .quality-good {{ background: #d1ecf1; color: #0c5460; }}
        .quality-fair {{ background: #fff3cd; color: #856404; }}
        .quality-poor {{ background: #f8d7da; color: #721c24; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background-color: #3498db;
            color: white;
            font-weight: bold;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        .recommendation {{
            margin: 15px 0;
            padding: 15px;
            border-radius: 5px;
            border-left: 4px solid;
        }}
        .recommendation.critical {{
            background-color: #f8d7da;
            border-color: #dc3545;
        }}
        .recommendation.warning {{
            background-color: #fff3cd;
            border-color: #ffc107;
        }}
        .recommendation.info {{
            background-color: #d1ecf1;
            border-color: #17a2b8;
        }}
        .recommendation.success {{
            background-color: #d4edda;
            border-color: #28a745;
        }}
        .recommendation-title {{
            font-weight: bold;
            margin-bottom: 5px;
        }}
        .error-sample {{
            background-color: #f8f9fa;
            padding: 10px;
            border-left: 3px solid #dc3545;
            margin: 10px 0;
            font-family: monospace;
            font-size: 12px;
        }}
        .badge {{
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: bold;
        }}
        .badge-danger {{ background-color: #dc3545; color: white; }}
        .badge-warning {{ background-color: #ffc107; color: black; }}
        .badge-info {{ background-color: #17a2b8; color: white; }}
        .timestamp {{
            color: #6c757d;
            font-size: 14px;
            text-align: right;
            margin-top: 20px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🔍 OFSAA FCCM Validation Report</h1>
        
        <div class="timestamp">
            Generated: {report_data['timestamp']}<br>
            File: {report_data['parse_metadata']['file_path']}<br>
            Table: {report_data['parse_metadata']['table_name']}
        </div>
        
        <h2>📊 Validation Summary</h2>
        <div class="summary-box">
            <div class="metric">
                <div class="metric-label">Total Records</div>
                <div class="metric-value">{report_data['summary']['total_records']:,}</div>
            </div>
            <div class="metric success">
                <div class="metric-label">Valid Records</div>
                <div class="metric-value">{report_data['summary']['valid_records']:,}</div>
            </div>
            <div class="metric warning">
                <div class="metric-label">Rejected Records</div>
                <div class="metric-value">{report_data['summary']['rejected_records']:,}</div>
            </div>
            <div class="metric">
                <div class="metric-label">Processing Time</div>
                <div class="metric-value">{report_data['summary']['processing_time_seconds']:.1f}s</div>
            </div>
        </div>
        
        <div class="quality-score {self._get_quality_class(report_data['summary']['data_quality_score'])}">
            Data Quality Score: {report_data['summary']['data_quality_score']:.1f}%
        </div>
        
        <h2>💡 Recommendations</h2>
        {self._format_recommendations_html(report_data['recommendations'])}
        
        <h2>📈 Error Analysis</h2>
        
        <h3>Errors by Type</h3>
        <table>
            <thead>
                <tr>
                    <th>Error Type</th>
                    <th>Count</th>
                    <th>Percentage</th>
                </tr>
            </thead>
            <tbody>
                {self._format_error_table(report_data['error_analysis']['error_by_type'], 
                                          report_data['error_analysis']['total_errors'])}
            </tbody>
        </table>
        
        <h3>Errors by Column</h3>
        <table>
            <thead>
                <tr>
                    <th>Column Name</th>
                    <th>Error Count</th>
                    <th>Percentage</th>
                </tr>
            </thead>
            <tbody>
                {self._format_error_table(report_data['error_analysis']['error_by_column'], 
                                          report_data['error_analysis']['total_errors'])}
            </tbody>
        </table>
        
        <h2>🔴 Sample Errors (Top 10)</h2>
        {self._format_sample_errors(report_data['top_errors'][:10])}
        
        <h2>📋 File Information</h2>
        <table>
            <tr>
                <th>Property</th>
                <th>Value</th>
            </tr>
            <tr>
                <td>File Path</td>
                <td>{report_data['parse_metadata']['file_path']}</td>
            </tr>
            <tr>
                <td>Table Name</td>
                <td>{report_data['parse_metadata']['table_name']}</td>
            </tr>
            <tr>
                <td>Encoding</td>
                <td>{report_data['parse_metadata']['encoding_used']}</td>
            </tr>
            <tr>
                <td>Expected Columns</td>
                <td>{report_data['parse_metadata']['expected_columns']}</td>
            </tr>
            <tr>
                <td>Actual Columns</td>
                <td>{report_data['parse_metadata']['actual_columns']}</td>
            </tr>
        </table>
        
        <div class="timestamp">
            <strong>Next Steps:</strong><br>
            1. Review rejected records in rejected_records.csv<br>
            2. Follow fix instructions in fix_instructions.txt<br>
            3. Revalidate after corrections<br>
            4. Load to OFSAA when quality score is above 95%
        </div>
    </div>
</body>
</html>
"""
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html)
    
    def _get_quality_class(self, score: float) -> str:
        """Get CSS class based on quality score"""
        if score >= 95:
            return 'quality-excellent'
        elif score >= 85:
            return 'quality-good'
        elif score >= 70:
            return 'quality-fair'
        else:
            return 'quality-poor'
    
    def _format_recommendations_html(self, recommendations: List[Dict]) -> str:
        """Format recommendations as HTML"""
        html = ""
        for rec in recommendations:
            severity_class = rec['severity'].lower()
            html += f"""
            <div class="recommendation {severity_class}">
                <div class="recommendation-title">
                    <span class="badge badge-{severity_class}">{rec['severity']}</span> 
                    {rec['message']}
                </div>
                <div><strong>Action:</strong> {rec['action']}</div>
            </div>
            """
        return html
    
    def _format_error_table(self, error_dict: Dict, total: int) -> str:
        """Format error dictionary as HTML table rows"""
        html = ""
        for key, count in list(error_dict.items())[:10]:  # Top 10
            percentage = (count / total * 100) if total > 0 else 0
            html += f"""
            <tr>
                <td>{key}</td>
                <td>{count:,}</td>
                <td>{percentage:.1f}%</td>
            </tr>
            """
        return html
    
    def _format_sample_errors(self, errors: List[Dict]) -> str:
        """Format sample errors as HTML"""
        html = ""
        for error in errors:
            html += f"""
            <div class="error-sample">
                <strong>Row {error['row']}</strong> - Column: {error['column']}<br>
                Error: {error['message']}<br>
                Actual Value: {error['actual_value']}<br>
                Expected: {error['expected_value']}<br>
                <strong>Fix:</strong> {error['fix_recommendation']}
            </div>
            """
        return html
    
    def _generate_excel_report(self, valid_df, rejected_df, errors, summary, output_path):
        """Generate Excel report with multiple sheets"""
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            
            # Sheet 1: Summary
            summary_df = pd.DataFrame([summary])
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Sheet 2: Valid Records
            if len(valid_df) > 0:
                valid_df.to_excel(writer, sheet_name='Valid Records', index=False)
            
            # Sheet 3: Rejected Records
            if len(rejected_df) > 0:
                rejected_df.to_excel(writer, sheet_name='Rejected Records', index=False)
            
            # Sheet 4: All Errors
            if errors:
                errors_df = pd.DataFrame([e.to_dict() for e in errors])
                errors_df.to_excel(writer, sheet_name='All Errors', index=False)
            
            # Sheet 5: Error Analysis
            error_analysis = self._create_error_analysis_df(errors)
            error_analysis.to_excel(writer, sheet_name='Error Analysis', index=False)
    
    def _create_error_analysis_df(self, errors: List) -> pd.DataFrame:
        """Create error analysis dataframe"""
        
        if not errors:
            return pd.DataFrame()
        
        error_by_type = {}
        error_by_column = {}
        
        for error in errors:
            error_by_type[error.error_type] = error_by_type.get(error.error_type, 0) + 1
            error_by_column[error.column_name] = error_by_column.get(error.column_name, 0) + 1
        
        analysis = []
        
        # By type
        for error_type, count in sorted(error_by_type.items(), key=lambda x: x[1], reverse=True):
            analysis.append({
                'Category': 'Error Type',
                'Name': error_type,
                'Count': count,
                'Percentage': f"{count/len(errors)*100:.1f}%"
            })
        
        # By column
        for column, count in sorted(error_by_column.items(), key=lambda x: x[1], reverse=True):
            analysis.append({
                'Category': 'Column',
                'Name': column,
                'Count': count,
                'Percentage': f"{count/len(errors)*100:.1f}%"
            })
        
        return pd.DataFrame(analysis)
    
    def _generate_fix_instructions(self, errors: List, output_path: str):
        """Generate fix instructions text file"""
        
        # Group errors by type
        errors_by_type = {}
        for error in errors:
            if error.error_type not in errors_by_type:
                errors_by_type[error.error_type] = []
            errors_by_type[error.error_type].append(error)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("OFSAA DATA VALIDATION - FIX INSTRUCTIONS\n")
            f.write("="*80 + "\n\n")
            
            f.write(f"Total Errors Found: {len(errors)}\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            for error_type, type_errors in errors_by_type.items():
                f.write("\n" + "="*80 + "\n")
                f.write(f"ERROR TYPE: {error_type} ({len(type_errors)} occurrences)\n")
                f.write("="*80 + "\n\n")
                
                # Group by column within error type
                by_column = {}
                for error in type_errors:
                    if error.column_name not in by_column:
                        by_column[error.column_name] = []
                    by_column[error.column_name].append(error)
                
                for column, col_errors in by_column.items():
                    f.write(f"\nColumn: {column} ({len(col_errors)} errors)\n")
                    f.write("-" * 80 + "\n")
                    
                    # Get unique fix recommendations
                    unique_fixes = set(e.fix_recommendation for e in col_errors if e.fix_recommendation)
                    if unique_fixes:
                        f.write("Fix Recommendation:\n")
                        for fix in unique_fixes:
                            f.write(f"  • {fix}\n")
                    
                    f.write("\nAffected Rows (first 20):\n")
                    for error in col_errors[:20]:
                        f.write(f"  Row {error.row_number}: '{error.actual_value}' -> {error.fix_recommendation}\n")
                    
                    if len(col_errors) > 20:
                        f.write(f"  ... and {len(col_errors) - 20} more rows\n")
                    f.write("\n")
            
            f.write("\n" + "="*80 + "\n")
            f.write("SUMMARY OF ACTIONS NEEDED\n")
            f.write("="*80 + "\n\n")
            
            f.write("1. Review rejected_records.csv for all rejected records\n")
            f.write("2. Apply fixes based on recommendations above\n")
            f.write("3. Revalidate corrected file\n")
            f.write("4. Load to OFSAA when quality score reaches 95%+\n\n")