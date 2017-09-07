using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using OfficeOpenXml;
using NPOI.HSSF.UserModel;
using NPOI.SS.UserModel;
using System.IO;

namespace ME_Batch.App
{
    class Excel : IDisposable
    {
        ExcelPackage xlsxExcel = null;
        HSSFWorkbook xlsExcel = null;
        public Excel(string file)
        {
            FileInfo fInfo = new FileInfo(file);
            if (fInfo.Extension == ".xls")
            {
                using (FileStream fs = new FileStream(fInfo.FullName, FileMode.Open, FileAccess.Read))
                {
                    xlsExcel = new HSSFWorkbook(fs);
                }
            }
            else if (fInfo.Extension == ".xlsx")
            {
                xlsxExcel = new ExcelPackage(fInfo);
            }
        }

        public string Cell(int col, int row)
        {
            if (xlsExcel != null)
            {
                return xlsExcel.GetSheetAt(0).GetRow(row).GetCell(col).StringCellValue;
            }
            else if (xlsxExcel != null)
            {
                ExcelWorksheet sheet = xlsxExcel.Workbook.Worksheets[1];
                return sheet.Cells[row + 1, col + 1].Value.ToString();
            }
            return null;
        }

        public int ColumnCount() {
            if (xlsExcel != null)
            {
                return xlsExcel.GetSheetAt(0).GetRow(0).LastCellNum;
            }
            else if (xlsxExcel != null)
            {
                ExcelWorksheet sheet = xlsxExcel.Workbook.Worksheets[1];
                return sheet.Dimension.Columns;
            }
            return -1;
        }

        public int RowCount()
        {
            if (xlsExcel != null)
            {
                return xlsExcel.GetSheetAt(0).LastRowNum;
            }
            else if (xlsxExcel != null)
            {
                ExcelWorksheet sheet = xlsxExcel.Workbook.Worksheets[1];
                return sheet.Dimension.End.Row - sheet.Dimension.Start.Row;
            }
            return -1;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        // Protected implementation of Dispose pattern.
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (xlsExcel != null)
                {
                    xlsExcel.Close();
                }
                else if (xlsxExcel != null)
                {
                    xlsxExcel.Dispose();
                }
            }
        }
    }
}
