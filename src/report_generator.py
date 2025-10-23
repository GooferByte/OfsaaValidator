# src/report_generator.py - COMPLETE FIXED VERSION WITH CORRECT INDENTATION

import json
from typing import Dict, List
from datetime import datetime
from pathlib import Path
import pandas as pd

class ReportGenerator:
    """Generate comprehensive validation reports with grouped errors"""
    
    def generate_report(self,
                       valid_df: pd.DataFrame,
                       rejected_df: pd.DataFrame,
                       errors: List,
                       summary: Dict,
                       parse_metadata: Dict,
                       output_dir: str):
        """Generate all report files"""
        
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Group errors for better reporting
        error_analysis = self._analyze_errors(errors)
        recommendations = self._generate_recommendations(summary, errors, rejected_df)
        
        report_data = {
            'summary': summary,
            'parse_metadata': parse_metadata,
            'error_analysis': error_analysis,
            'recommendations': recommendations,
            'grouped_errors': self._group_errors_for_display(errors),
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
            'unique_error_types': len(error_by_type),
            'affected_columns': len(error_by_column),
            'error_by_type': dict(sorted(error_by_type.items(), key=lambda x: x[1], reverse=True)),
            'error_by_column': dict(sorted(error_by_column.items(), key=lambda x: x[1], reverse=True)),
            'worst_rows': [{'row': row, 'error_count': count} for row, count in worst_rows]
        }
    
    def _group_errors_for_display(self, errors: List) -> List[Dict]:
        """Group errors by type and column for concise display"""
        
        error_groups = {}
        for error in errors:
            key = (error.error_type, error.column_name)
            if key not in error_groups:
                error_groups[key] = {
                    'error_type': error.error_type,
                    'column': error.column_name,
                    'count': 0,
                    'fix_recommendation': error.fix_recommendation,
                    'sample_rows': [],
                    'sample_values': []
                }
            
            group = error_groups[key]
            group['count'] += 1
            
            # Keep first 5 sample rows
            if len(group['sample_rows']) < 5:
                group['sample_rows'].append(error.row_number)
            
            # Keep first 3 unique sample values
            value_str = str(error.actual_value)[:50]
            if value_str not in group['sample_values'] and len(group['sample_values']) < 3:
                group['sample_values'].append(value_str)
        
        # Convert to list and sort by count
        grouped_list = list(error_groups.values())
        grouped_list.sort(key=lambda x: x['count'], reverse=True)
        
        return grouped_list
    
    def _generate_recommendations(self, summary: Dict, errors: List, rejected_df: pd.DataFrame) -> List[str]:
        """Generate actionable recommendations - CONCISE VERSION"""
        
        recommendations = []
        
        # 1. Overall Data Quality Assessment
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
        
        if not errors:
            return recommendations
        
        # 2. Group Errors by Type and Column
        error_groups = {}
        for error in errors:
            key = (error.error_type, error.column_name)
            if key not in error_groups:
                error_groups[key] = {
                    'count': 0,
                    'sample_fix': error.fix_recommendation,
                    'error_type': error.error_type,
                    'column': error.column_name
                }
            error_groups[key]['count'] += 1
        
        # 3. Sort by frequency (most common first)
        sorted_groups = sorted(error_groups.items(), key=lambda x: x[1]['count'], reverse=True)
        
        # 4. Generate Top 5 Recommendations Only
        for idx, ((error_type, column), info) in enumerate(sorted_groups[:5], 1):
            
            severity = 'HIGH' if info['count'] > 100 else 'MEDIUM' if info['count'] > 10 else 'LOW'
            
            # Create concise message
            if error_type == 'VALUE_MISSING':
                message = f"{info['count']} records missing mandatory field '{column}'"
                action = info['sample_fix']
            elif error_type == 'INVALID_DATA_TYPE':
                message = f"{info['count']} records have invalid data type in '{column}'"
                action = info['sample_fix']
            elif error_type == 'LENGTH_EXCEEDED':
                message = f"{info['count']} records exceed length limit in '{column}'"
                action = f"Truncate values in column '{column}' to meet length requirements"
            elif error_type == 'INVALID_FORMAT':
                message = f"{info['count']} records have invalid format in '{column}'"
                action = info['sample_fix']
            else:
                message = f"{info['count']} errors in '{column}'"
                action = "Review and fix data formatting"
            
            recommendations.append({
                'severity': severity,
                'message': message,
                'action': action,
                'priority': idx + 1
            })
        
        # 5. Summary if more errors exist
        if len(sorted_groups) > 5:
            remaining = len(sorted_groups) - 5
            recommendations.append({
                'severity': 'INFO',
                'message': f'{remaining} additional error types found',
                'action': f'Review rejected_records.csv for complete details',
                'priority': 10
            })
        
        return recommendations
    
    def _generate_html_report(self, report_data: Dict, output_path: str):
        """Generate HTML report with grouped errors"""
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>OFSAA Validation Report</title>
    <meta charset="UTF-8">
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
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
        .recommendation.high {{
            background-color: #f8d7da;
            border-color: #dc3545;
        }}
        .recommendation.warning {{
            background-color: #fff3cd;
            border-color: #ffc107;
        }}
        .recommendation.medium {{
            background-color: #fff3cd;
            border-color: #ffc107;
        }}
        .recommendation.info {{
            background-color: #d1ecf1;
            border-color: #17a2b8;
        }}
        .recommendation.low {{
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
        .error-group {{
            background-color: #f8f9fa;
            padding: 15px;
            margin: 15px 0;
            border-left: 4px solid #dc3545;
            border-radius: 4px;
        }}
        .error-group h4 {{
            margin-top: 0;
            color: #333;
        }}
        .error-group p {{
            margin: 8px 0;
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
        .badge-success {{ background-color: #28a745; color: white; }}
        .timestamp {{
            color: #6c757d;
            font-size: 14px;
            text-align: right;
            margin-top: 20px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }}
        .stat-card {{
            background: #f8f9fa;
            padding: 15px;
            border-radius: 5px;
            border-left: 4px solid #3498db;
        }}
        .stat-card h4 {{
            margin: 0 0 10px 0;
            color: #555;
        }}
        .stat-card .value {{
            font-size: 24px;
            font-weight: bold;
            color: #333;
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
        
        <h2>📈 Error Analysis</h2>
        <div class="stats-grid">
            <div class="stat-card">
                <h4>Total Errors</h4>
                <div class="value">{report_data['error_analysis']['total_errors']:,}</div>
            </div>
            <div class="stat-card">
                <h4>Error Types</h4>
                <div class="value">{report_data['error_analysis']['unique_error_types']}</div>
            </div>
            <div class="stat-card">
                <h4>Affected Columns</h4>
                <div class="value">{report_data['error_analysis']['affected_columns']}</div>
            </div>
        </div>
        
        <h2>💡 Recommendations</h2>
        {self._format_recommendations_html(report_data['recommendations'])}
        
        <h2>🔴 Grouped Errors (Top 10)</h2>
        {self._format_grouped_errors_html(report_data.get('grouped_errors', [])[:10])}
        
        <h2>📋 Error Distribution</h2>
        
        <h3>By Error Type</h3>
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
        
        <h3>By Column (Top 10)</h3>
        <table>
            <thead>
                <tr>
                    <th>Column Name</th>
                    <th>Error Count</th>
                    <th>Percentage</th>
                </tr>
            </thead>
            <tbody>
                {self._format_error_table(dict(list(report_data['error_analysis']['error_by_column'].items())[:10]), 
                                          report_data['error_analysis']['total_errors'])}
            </tbody>
        </table>
        
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
            1. Review rejected_records.csv for all rejected records<br>
            2. Follow fix_instructions.txt for grouped error fixes<br>
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
                    <span class="badge badge-{self._get_badge_class(severity_class)}">{rec['severity']}</span> 
                    {rec['message']}
                </div>
                <div><strong>Action:</strong> {rec['action']}</div>
            </div>
            """
        return html
    
    def _get_badge_class(self, severity: str) -> str:
        """Get badge class for severity"""
        mapping = {
            'critical': 'danger',
            'high': 'danger',
            'warning': 'warning',
            'medium': 'warning',
            'info': 'info',
            'low': 'info',
            'success': 'success'
        }
        return mapping.get(severity.lower(), 'info')
    
    def _format_grouped_errors_html(self, grouped_errors: List[Dict]) -> str:
        """Format grouped errors as HTML"""
        html = ""
        for group in grouped_errors:
            # Format sample values
            sample_values_html = ""
            if group['sample_values']:
                sample_values_str = ", ".join([f"'{v}'" for v in group['sample_values']])
                sample_values_html = f"<p><strong>Sample Values:</strong> {sample_values_str}</p>"
            
            # Format sample rows
            sample_rows_str = ', '.join(map(str, group['sample_rows']))
            
            html += f"""
            <div class="error-group">
                <h4>
                    <span class="badge badge-danger">{group['error_type']}</span> 
                    Column: {group['column']}
                </h4>
                <p><strong>Occurrences:</strong> {group['count']:,}</p>
                <p><strong>Fix:</strong> {group['fix_recommendation']}</p>
                <p><strong>Sample Rows:</strong> {sample_rows_str}</p>
                {sample_values_html}
            </div>
            """
        return html
    
    def _format_error_table(self, error_dict: Dict, total: int) -> str:
        """Format error dictionary as HTML table rows"""
        html = ""
        for key, count in list(error_dict.items()):
            percentage = (count / total * 100) if total > 0 else 0
            html += f"""
            <tr>
                <td>{key}</td>
                <td>{count:,}</td>
                <td>{percentage:.1f}%</td>
            </tr>
            """
        return html
    
    def _generate_excel_report(self, valid_df, rejected_df, errors, summary, output_path):
        """Generate Excel report with multiple sheets"""
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            
            # Sheet 1: Summary
            summary_df = pd.DataFrame([summary])
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Sheet 2: Valid Records (first 10000 rows)
            if len(valid_df) > 0:
                valid_df.head(10000).to_excel(writer, sheet_name='Valid Records', index=False)
            
            # Sheet 3: Rejected Records (first 10000 rows)
            if len(rejected_df) > 0:
                rejected_df.head(10000).to_excel(writer, sheet_name='Rejected Records', index=False)
            
            # Sheet 4: Grouped Errors
            if errors:
                grouped_errors = self._group_errors_for_display(errors)
                grouped_df = pd.DataFrame(grouped_errors)
                grouped_df.to_excel(writer, sheet_name='Grouped Errors', index=False)
            
            # Sheet 5: Error Analysis
            error_analysis = self._create_error_analysis_df(errors)
            if not error_analysis.empty:
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
        """Generate CONCISE fix instructions"""
        
        # Group errors by type and column
        error_groups = {}
        for error in errors:
            key = (error.error_type, error.column_name)
            if key not in error_groups:
                error_groups[key] = {
                    'errors': [],
                    'fix': error.fix_recommendation
                }
            error_groups[key]['errors'].append(error)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("OFSAA DATA VALIDATION - FIX INSTRUCTIONS\n")
            f.write("=" * 80 + "\n\n")
            
            f.write(f"Total Errors Found: {len(errors):,}\n")
            f.write(f"Total Error Groups: {len(error_groups)}\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("=" * 80 + "\n")
            f.write("SUMMARY OF ISSUES (Grouped by Error Type and Column)\n")
            f.write("=" * 80 + "\n\n")
            
            # Sort by frequency
            sorted_groups = sorted(error_groups.items(), key=lambda x: len(x[1]['errors']), reverse=True)
            
            for idx, ((error_type, column), info) in enumerate(sorted_groups, 1):
                f.write(f"\n{idx}. {error_type} - Column: {column}\n")
                f.write("-" * 80 + "\n")
                f.write(f"   Occurrences: {len(info['errors']):,}\n")
                f.write(f"   Fix: {info['fix']}\n")
                
                # Show first 10 affected rows
                f.write("   Affected Rows (first 10): ")
                row_numbers = [str(e.row_number) for e in info['errors'][:10]]
                f.write(", ".join(row_numbers))
                if len(info['errors']) > 10:
                    f.write(f" ... and {len(info['errors']) - 10:,} more")
                f.write("\n")
                
                # Show sample values (first 3 unique)
                sample_values = list(set([str(e.actual_value)[:50] for e in info['errors']]))[:3]
                if sample_values:
                    # Format sample values without nested f-strings
                    formatted_values = ", ".join([f"'{v}'" for v in sample_values])
                    f.write(f"   Sample Values: {formatted_values}\n")
            
            f.write("\n\n" + "=" * 80 + "\n")
            f.write("QUICK FIX GUIDE (Top 5 Issues)\n")
            f.write("=" * 80 + "\n\n")
            
            # Quick action items
            f.write("PRIORITY ACTIONS:\n\n")
            
            for idx, ((error_type, column), info) in enumerate(sorted_groups[:5], 1):
                f.write(f"{idx}. Fix {len(info['errors']):,} occurrences of {error_type} in column '{column}'\n")
                f.write(f"   → {info['fix']}\n\n")
            
            f.write("\n" + "=" * 80 + "\n")
            f.write("DETAILED ERROR LIST\n")
            f.write("=" * 80 + "\n")
            f.write("See 'rejected_records.csv' for complete list with all row numbers and values\n")
            f.write("=" * 80 + "\n")