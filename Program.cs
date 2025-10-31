using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using MySql.Data.MySqlClient;
using System.Text;
using System.Globalization;
using System.Diagnostics;
using System.Data;
using System.Text.Json;

namespace ExcelMonitoring1
{
    class Program
    {
        // MySQL integration flags
        static bool MySqlEnabled = false;
        static string? MySqlConnectionString = null;
        static string? MySqlTableName = null;
        static string? MySqlDatabaseName = null;

        // Application settings
        static AppConfig config = new AppConfig();
        static List<OperationLog> operationLogs = new List<OperationLog>();

        static void Main(string[] args)
        {
            try
            {
                AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException!;
                AppDomain.CurrentDomain.ProcessExit += CurrentDomain_ProcessExit!;

                // Load configuration
                config.Load();

                // Handle command line arguments
                if (args.Length > 0)
                {
                    HandleCommandLineArgs(args);
                    return;
                }

                RunInteractiveMode();
            }
            catch (Exception ex)
            {
                HandleFatalError("Kesalahan kritis pada aplikasi", ex);
            }
        }

        #region Global Error Handlers
        static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            Exception? ex = e.ExceptionObject as Exception;
            HandleFatalError("Kesalahan tidak tertangani", ex);
        }

        static void CurrentDomain_ProcessExit(object sender, EventArgs e)
        {
            CleanupResources();
        }

        static void HandleFatalError(string context, Exception? ex)
        {
            try
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"\n=== {context.ToUpper()} ===");
                Console.WriteLine($"Waktu: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");

                if (ex != null)
                {
                    Console.WriteLine($"Tipe Error: {ex.GetType().Name}");
                    Console.WriteLine($"Pesan: {ex.Message}");

                    // Log detail untuk debugging
                    LogOperation("FATAL_ERROR", $"{context}: {ex.GetType().Name} - {ex.Message}");

                    if (ex.InnerException != null)
                    {
                        Console.WriteLine($"Inner Exception: {ex.InnerException.Message}");
                    }

                    // Tampilkan stack trace hanya dalam mode debug
#if DEBUG
                    Console.WriteLine($"Stack Trace:\n{ex.StackTrace}");
#endif
                }

                Console.WriteLine("\nAplikasi akan ditutup...");
                Console.ResetColor();

                // Log ke file untuk investigasi
                LogToFile($"FATAL: {context} - {ex?.Message}");
            }
            finally
            {
                CleanupResources();
                Environment.Exit(1);
            }
        }

        static void LogToFile(string message)
        {
            try
            {
                string logDir = "ErrorLogs";
                Directory.CreateDirectory(logDir);
                string logFile = Path.Combine(logDir, $"error_{DateTime.Now:yyyyMMdd}.txt");
                File.AppendAllText(logFile, $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}\n");
            }
            catch
            {
                // Ignore errors in error logging
            }
        }
        #endregion

        #region Enhanced Error Handling Methods
        static T ExecuteWithRetry<T>(Func<T> operation, string operationName, int maxRetries = 3, int delayMs = 1000)
        {
            int retryCount = 0;
            while (true)
            {
                try
                {
                    return operation();
                }
                catch (Exception ex) when (retryCount < maxRetries)
                {
                    retryCount++;
                    ShowWarning($"Percobaan {retryCount}/{maxRetries} gagal untuk {operationName}: {ex.Message}");

                    if (retryCount < maxRetries)
                    {
                        ShowInfo($"Menunggu {delayMs}ms sebelum mencoba lagi...");
                        Thread.Sleep(delayMs);
                        delayMs *= 2; // Exponential backoff
                    }
                }
            }
        }

        static bool ExecuteWithTimeout(Action operation, string operationName, int timeoutMs = 30000)
        {
            var cts = new CancellationTokenSource();
            cts.CancelAfter(timeoutMs);

            try
            {
                var task = Task.Run(() =>
                {
                    try
                    {
                        operation();
                        return true;
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        LogOperation("TIMEOUT_ERROR", $"{operationName}: {ex.Message}");
                        return false;
                    }
                }, cts.Token);

                if (task.Wait(timeoutMs))
                {
                    return task.Result;
                }
                else
                {
                    throw new TimeoutException($"{operationName} timeout setelah {timeoutMs}ms");
                }
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException($"{operationName} timeout setelah {timeoutMs}ms");
            }
        }

        static void ValidateFileAccess(string filePath)
        {
            if (string.IsNullOrWhiteSpace(filePath))
                throw new ArgumentException("Path file tidak boleh kosong");

            // Jika file tidak ada, kita tidak perlu validasi akses karena akan dibuat
            if (!File.Exists(filePath))
                return;

            // Cek apakah file bisa diakses
            try
            {
                using var fs = File.Open(filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
            }
            catch (IOException ex)
            {
                throw new InvalidOperationException($"File sedang digunakan oleh proses lain: {ex.Message}");
            }
            catch (UnauthorizedAccessException ex)
            {
                throw new UnauthorizedAccessException($"Tidak memiliki akses ke file: {ex.Message}");
            }
        }

        static void ValidateMySqlConnection()
        {
            if (!MySqlEnabled)
                throw new InvalidOperationException("MySQL tidak diaktifkan");

            if (string.IsNullOrEmpty(MySqlConnectionString))
                throw new InvalidOperationException("String koneksi MySQL tidak ditemukan");

            try
            {
                using var connection = new MySqlConnection(MySqlConnectionString);
                connection.Open();

                // Test basic query
                using var cmd = connection.CreateCommand();
                cmd.CommandText = "SELECT 1";
                cmd.ExecuteScalar();
            }
            catch (MySqlException ex)
            {
                throw new InvalidOperationException($"Koneksi MySQL gagal: {ex.Message}", ex);
            }
        }
        #endregion

        #region Core Application Flow
        
        static void RunInteractiveMode()
        {
            try
            {
                DrawTitle("Batam Schneider Entry Data (Batam S.E.D) Program | " +
                    "Created by : Samuel Hasiholan Omega Purba, S. Tr. T. || Powered by : DeepSeek.AI | " +
                    "Bachelor Robotic's Technology and Artificial's Intelligence of Batam State Polytechnic");

                // Apply Russian (RUS-95) environment
                SetRussianEnvironment();

                // Setup file and sheet dengan error handling yang lebih baik
                try
                {
                    var (filePath, sheetName) = SetupWorkbook();

                    // Tampilkan status file
                    ShowFileStatus(filePath);

                    // MySQL configuration
                    try
                    {
                        ConfigureMySQL(sheetName);
                    }
                    catch (Exception mysqlEx)
                    {
                        ShowWarning($"MySQL configuration skipped: {mysqlEx.Message}");
                        LogOperation("MYSQL_SKIP", $"MySQL config skipped: {mysqlEx.Message}");
                    }

                    // Main program loop
                    RunMainMenu(filePath, sheetName);
                }
                catch (Exception setupEx)
                {
                    ShowError($"Setup gagal: {setupEx.Message}");
                    ShowInfo("Silakan coba lagi dengan path file yang berbeda.");

                    // Coba setup ulang
                    Thread.Sleep(2000);
                    RunInteractiveMode();
                }
            }
            catch (Exception ex)
            {
                HandleFatalError("Kesalahan dalam mode interaktif", ex);
            }
        }

        static void SetRussianEnvironment()
        {
            try
            {
                var cp1251 = Encoding.GetEncoding(1251);
                Console.InputEncoding = cp1251;
                Console.OutputEncoding = cp1251;

                var ru = new CultureInfo("ru-RU");
                CultureInfo.CurrentCulture = ru;
                CultureInfo.CurrentUICulture = ru;

                LogOperation("SYSTEM", "Russian environment berhasil diatur");
            }
            catch (Exception ex)
            {
                LogOperation("WARNING", $"Gagal mengatur environment Rusia: {ex.Message}");
                ShowWarning($"Environment Rusia tidak tersedia: {ex.Message}");
            }
        }

        static (string filePath, string sheetName) SetupWorkbook()
        {
            try
            {
                Console.Write("Masukkan path file (tekan Enter untuk default): ");
                string inputPath = Console.ReadLine() ?? "";
                string filePath = string.IsNullOrWhiteSpace(inputPath)
                    ? config.DefaultFilePath ?? "Batam_Schneider_Entry_Data_File1.xlsx"
                    : inputPath.Trim();

                // Jika tidak ada ekstensi, tambahkan .xlsx
                if (!Path.HasExtension(filePath))
                {
                    filePath += ".xlsx";
                    ShowInfo($"Menambahkan ekstensi .xlsx: {filePath}");
                }

                Console.Write("Masukkan nama data (nama sheet): ");
                string? inputSheet = Console.ReadLine();
                string sheetName = string.IsNullOrWhiteSpace(inputSheet)
                    ? config.DefaultSheetName ?? "Data1"
                    : inputSheet.Trim();

                // Validasi dan normalisasi path file
                if (!Path.IsPathRooted(filePath))
                {
                    filePath = Path.Combine(Directory.GetCurrentDirectory(), filePath);
                    ShowInfo($"Menggunakan path absolute: {filePath}");
                }

                // Buat directory jika belum ada
                string? directory = Path.GetDirectoryName(filePath);
                if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                    ShowInfo($"Directory dibuat: {directory}");
                }

                // Validasi akses file - jangan throw error jika file tidak ada
                //ValidateFileAccess(filePath);

                EnsureWorkbookAndSheet(filePath, sheetName);
                EnsureHeaderRow(filePath, sheetName);

                LogOperation("SETUP", $"Workbook setup berhasil: {filePath}, Sheet: {sheetName}");
                return (filePath, sheetName);
            }
            catch (Exception ex)
            {
                LogOperation("ERROR", $"Setup workbook gagal: {ex.Message}");
                throw new Exception($"Gagal menyiapkan workbook: {ex.Message}", ex);
            }
        }

        static void RunMainMenu(string filePath, string sheetName)
        {
            while (true)
            {
                Console.WriteLine();
                ShowMainMenu();

                Console.Write("Pilihan: ");
                string? sel = Console.ReadLine();
                if (string.IsNullOrWhiteSpace(sel)) continue;

                ProcessMenuSelection(sel.Trim().ToUpperInvariant(), filePath, sheetName);
            }
        }

        static void ShowMainMenu()
        {
            var menuItems = new[]
            {
                "(A) Impor CSV",
                "(C) Tambah baris",
                "(R) Tampilkan baris",
                "(U) Perbarui baris",
                "(D) Hapus baris",
                "(F) Cari data",
                "(B) Batch operations",
                "(M) Monitor langsung",
                "(S) Settings MySQL",
                "(DB) MySQL Auto-Connect",
                "(MS) MySQL System Info",
                "(MB) MySQL Backup Data",
                "(MR) MySQL Restore Data",
                "(ME) MySQL Export to Excel",
                "(CF) Configuration",
                "(LG) View Logs",
                "(Q) Keluar"
            };

            DrawMenu(menuItems);
        }

        static void ProcessMenuSelection(string selection, string filePath, string sheetName)
        {
            try
            {
                switch (selection)
                {
                    case "A": ImportCsvMenu(filePath, sheetName); break;
                    case "C": AddRowMenu(filePath, sheetName); break;
                    case "R": ShowRowsMenu(filePath, sheetName); break;
                    case "U": UpdateRowMenu(filePath, sheetName); break;
                    case "D": DeleteRowMenu(filePath, sheetName); break;
                    case "F": SearchDataMenu(filePath, sheetName); break;
                    case "B": BatchOperationsMenu(filePath, sheetName); break;
                    case "M": LiveMonitor(filePath, sheetName); break;
                    case "S": ShowMySqlConfigurationMenu(sheetName); break;
                    case "DB": AutoConnectToMySQL(sheetName); break;
                    case "MS": ShowMySqlSystemInfo(); break;
                    case "MB": BackupMySqlData(); break;
                    case "MR": RestoreMySqlData(); break;
                    case "ME": ExportMySqlToExcel(filePath, sheetName); break;
                    case "CF": ShowConfigurationMenu(); break;
                    case "LG": ShowLogsMenu(); break;
                    case "Q":
                        ExitProgram();
                        return;
                    default:
                        ShowError("Pilihan tidak dikenal.");
                        break;
                }
            }
            catch (Exception ex)
            {
                LogOperation("ERROR", $"Menu {selection}: {ex.Message}");
                ShowError($"Kesalahan: {ex.Message}");
            }
        }
        #endregion

        #region Command Line Handling
        static void HandleCommandLineArgs(string[] args)
        {
            try
            {
                switch (args[0].ToLower())
                {
                    case "--import":
                        if (args.Length >= 3)
                            CommandLineImport(args[1], args[2]);
                        break;
                    case "--export":
                        if (args.Length >= 2)
                            CommandLineExport(args[1]);
                        break;
                    case "--backup":
                        CommandLineBackup();
                        break;
                    case "--help":
                        ShowCommandLineHelp();
                        break;
                    default:
                        Console.WriteLine("Invalid command. Use --help for usage information.");
                        break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }

        static void CommandLineImport(string filePath, string csvPath)
        {
            Console.WriteLine($"Importing {csvPath} to {filePath}...");
            // Implementation for command line import
        }

        static void CommandLineExport(string filePath)
        {
            Console.WriteLine($"Exporting from {filePath}...");
            // Implementation for command line export
        }

        static void CommandLineBackup()
        {
            Console.WriteLine("Creating backup...");
            BackupMySqlData();
        }

        static void ShowCommandLineHelp()
        {
            Console.WriteLine(@"Batam S.E.D Program - Command Line Usage:
--import <excel> <csv>    Import CSV to Excel
--export <excel>          Export Excel data
--backup                  Create MySQL backup
--help                    Show this help");
        }
        #endregion

        #region UI Helpers
        static void DrawTitle(string title)
        {
            var orig = Console.ForegroundColor;
            Console.Clear();
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine(new string('=', Math.Min(title.Length + 8, 120)));
            Console.WriteLine($" {title}");
            Console.WriteLine(new string('=', Math.Min(title.Length + 8, 120)));
            Console.WriteLine();
            Console.ForegroundColor = orig;
        }
        static void ShowFileStatus(string filePath)
        {
            if (File.Exists(filePath))
            {
                var fileInfo = new FileInfo(filePath);
                ShowSuccess($"File ditemukan: {filePath}");
                ShowInfo($"Ukuran: {fileInfo.Length} bytes, Dibuat: {fileInfo.CreationTime}");
            }
            else
            {
                ShowWarning($"File tidak ditemukan: {filePath}");
                ShowInfo("File akan dibuat otomatis...");
            }
        }
        static void DrawMenu(string[] lines)
        {
            var orig = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.DarkGreen;
            Console.WriteLine("Menu:");
            Console.ForegroundColor = ConsoleColor.Gray;
            foreach (var l in lines) Console.WriteLine($" {l}");
            Console.ForegroundColor = orig;
        }

        static void PrintRowsTable(List<string[]> rows)
        {
            // Determine column count using header row if present; otherwise max columns
            int colCount = rows.Max(r => r.Length);
            var headers = rows.Count > 0 ? rows[0] : Enumerable.Range(0, colCount).Select(i => $"Col{i + 1}").ToArray();

            // compute max widths (cap to30 for readability)
            int[] widths = new int[colCount];
            for (int c = 0; c < colCount; c++)
            {
                int max = 0;
                for (int r = 0; r < rows.Count; r++)
                {
                    var cell = c < rows[r].Length ? rows[r][c] : string.Empty;
                    max = Math.Max(max, (cell ?? string.Empty).Length);
                }
                widths[c] = Math.Min(Math.Max(max, headers.ElementAtOrDefault(c)?.Length ?? 0), 30);
            }

            // print header (use first row as header if it matches EntryRecord.Headers; otherwise print generated)
            bool firstIsHeaders = headers.Length >= EntryRecord.Headers.Length + 1 && EntryRecord.Headers.SequenceEqual(headers.Skip(1).Take(EntryRecord.Headers.Length));
            string[] hdrToPrint = firstIsHeaders ? headers : (new[] { "Id" }).Concat(EntryRecord.Headers.Take(Math.Max(0, colCount - 1))).ToArray();

            // draw header
            Console.ForegroundColor = ConsoleColor.White;
            for (int c = 0; c < colCount; c++)
            {
                var h = c < hdrToPrint.Length ? hdrToPrint[c] : $"Col{c + 1}";
                Console.Write($"| {Truncate(h, widths[c]).PadRight(widths[c])} ");
            }
            Console.WriteLine("|");
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.WriteLine(new string('-', colCount * (3 + widths.Max())));

            // draw rows (skip header row from file if it equals EntryRecord.Headers)
            int start = firstIsHeaders ? 1 : 0;
            Console.ForegroundColor = ConsoleColor.Gray;
            for (int r = start; r < rows.Count; r++)
            {
                var row = rows[r];
                for (int c = 0; c < colCount; c++)
                {
                    var cell = c < row.Length ? (row[c] ?? string.Empty) : string.Empty;
                    Console.Write($"| {Truncate(cell, widths[c]).PadRight(widths[c])} ");
                }
                Console.WriteLine($"| ({r + 1})");
            }
            Console.ResetColor();
        }

        static string Truncate(string s, int max) => s.Length <= max ? s : s.Substring(0, max - 3) + "...";

        static void ShowInfo(string message)
        {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine($"ℹ {message}");
            Console.ResetColor();
            LogOperation("INFO", message);
        }

        static void ShowSuccess(string message)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"✓ {message}");
            Console.ResetColor();
            LogOperation("SUCCESS", message);
        }

        static void ShowWarning(string message)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"⚠ {message}");
            Console.ResetColor();
            LogOperation("WARNING", message);
        }

        static void ShowError(string message)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"✗ {message}");
            Console.ResetColor();
            LogOperation("ERROR", message);
        }

        static void LogOperation(string operationType, string description)
        {
            try
            {
                var log = new OperationLog
                {
                    Timestamp = DateTime.Now,
                    OperationType = operationType,
                    Description = description
                };
                operationLogs.Add(log);

                // Keep only last 1000 logs
                if (operationLogs.Count > 1000)
                    operationLogs.RemoveAt(0);
            }
            catch
            {
                // Ignore logging errors
            }
        }

        static void CleanupResources()
        {
            try
            {
                config.Save();
                LogOperation("SYSTEM", "Application shutdown dan cleanup");

                // Cleanup temporary files if any
                CleanupTempFiles();
            }
            catch
            {
                // Ignore cleanup errors
            }
        }

        static void CleanupTempFiles()
        {
            try
            {
                var tempDir = Path.GetTempPath();
                var tempFiles = Directory.GetFiles(tempDir, "Batam_SED_*.tmp");
                foreach (var file in tempFiles)
                {
                    try
                    {
                        if (File.Exists(file) && (DateTime.Now - File.GetCreationTime(file)).TotalHours > 24)
                        {
                            File.Delete(file);
                        }
                    }
                    catch
                    {
                        // Ignore individual file deletion errors
                    }
                }
            }
            catch
            {
                // Ignore cleanup errors
            }
        }

        static void ExitProgram()
        {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Terima kasih telah menggunakan Batam S.E.D Program!");
            Console.ResetColor();

            CleanupResources();
            Environment.Exit(0);
        }
        #endregion

        #region Excel File Operations
        static void EnsureWorkbookAndSheet(string path, string sheetName)
        {
            try
            {
                // Validasi parameter
                if (string.IsNullOrWhiteSpace(path))
                    throw new ArgumentException("Path tidak boleh kosong");

                if (string.IsNullOrWhiteSpace(sheetName))
                    throw new ArgumentException("Nama sheet tidak boleh kosong");

                // Create file if missing
                if (!File.Exists(path))
                {
                    ShowInfo($"Membuat file Excel baru: {path}");

                    try
                    {
                        using var createDoc = SpreadsheetDocument.Create(path, SpreadsheetDocumentType.Workbook);
                        var createWbPart = createDoc.AddWorkbookPart();
                        createWbPart.Workbook = new Workbook();
                        var worksheetPart = createWbPart.AddNewPart<WorksheetPart>();
                        worksheetPart.Worksheet = new Worksheet(new SheetData());
                        var sheets = createWbPart.Workbook.AppendChild(new Sheets());
                        var sheet = new Sheet()
                        {
                            Id = createWbPart.GetIdOfPart(worksheetPart),
                            SheetId = 1,
                            Name = sheetName
                        };
                        sheets.Append(sheet);
                        createWbPart.Workbook.Save();

                        LogOperation("FILE_CREATE", $"File Excel dibuat: {path}");
                        ShowSuccess($"File Excel berhasil dibuat: {path}");
                        return;
                    }
                    catch (Exception ex)
                    {
                        throw new Exception($"Gagal membuat file Excel: {ex.Message}", ex);
                    }
                }

                // Open existing and ensure sheet exists
                try
                {
                    using var doc = SpreadsheetDocument.Open(path, true);
                    var wbPart = doc.WorkbookPart ?? doc.AddWorkbookPart();
                    wbPart.Workbook ??= new Workbook();

                    var sheetsElement = wbPart.Workbook.GetFirstChild<Sheets>() ?? wbPart.Workbook.AppendChild(new Sheets());

                    var existing = sheetsElement.Elements<Sheet>().FirstOrDefault(s =>
                        string.Equals(s.Name?.Value, sheetName, StringComparison.OrdinalIgnoreCase));

                    if (existing == null)
                    {
                        var worksheetPart = wbPart.AddNewPart<WorksheetPart>();
                        worksheetPart.Worksheet = new Worksheet(new SheetData());
                        uint newId = sheetsElement.Elements<Sheet>().Select(s => s.SheetId?.Value ?? 0u).DefaultIfEmpty(0u).Max() + 1;
                        var sheet = new Sheet()
                        {
                            Id = wbPart.GetIdOfPart(worksheetPart),
                            SheetId = newId,
                            Name = sheetName
                        };
                        sheetsElement.Append(sheet);
                        wbPart.Workbook.Save();

                        LogOperation("SHEET_CREATE", $"Sheet dibuat: {sheetName}");
                        ShowSuccess($"Sheet berhasil dibuat: {sheetName}");
                    }
                    else
                    {
                        ShowInfo($"Sheet sudah ada: {sheetName}");
                    }
                }
                catch (OpenXmlPackageException ex)
                {
                    throw new Exception($"Format file Excel tidak valid: {ex.Message}", ex);
                }
            }
            catch (IOException ex)
            {
                throw new Exception($"File Excel tidak dapat diakses: {ex.Message}", ex);
            }
            catch (Exception ex)
            {
                throw new Exception($"Gagal memastikan workbook dan sheet: {ex.Message}", ex);
            }
        }

        static void EnsureHeaderRow(string path, string sheetName)
        {
            try
            {
                using var doc = SpreadsheetDocument.Open(path, true);
                var wbPart = doc.WorkbookPart ?? throw new InvalidOperationException("Workbook tidak ditemukan");
                var wsPart = GetWorksheetPartByName(wbPart, sheetName) ?? throw new InvalidOperationException("Worksheet tidak ditemukan");
                var sheetData = wsPart.Worksheet.GetFirstChild<SheetData>() ?? wsPart.Worksheet.AppendChild(new SheetData());

                var firstRow = sheetData.Elements<Row>().FirstOrDefault();
                var headers = EntryRecord.Headers;
                bool needsHeader = true;

                if (firstRow != null)
                {
                    var firstValues = firstRow.Elements<Cell>().Select(c =>
                        GetCellText(c, wbPart.SharedStringTablePart?.SharedStringTable) ?? string.Empty).ToArray();

                    // if sheet already has Id + headers
                    if (firstValues.Length >= headers.Length + 1 &&
                        headers.SequenceEqual(firstValues.Skip(1).Take(headers.Length)))
                        needsHeader = false;
                }

                if (needsHeader)
                {
                    var headerRow = new Row() { RowIndex = 1u };

                    // add Id as first column
                    int sstIndex0 = InsertSharedStringItem(wbPart, "Id");
                    headerRow.Append(new Cell()
                    {
                        CellReference = GetCellReference(0, 1),
                        DataType = new EnumValue<CellValues>(CellValues.SharedString),
                        CellValue = new CellValue(sstIndex0.ToString())
                    });

                    for (int i = 0; i < headers.Length; i++)
                    {
                        int sstIndex = InsertSharedStringItem(wbPart, headers[i]);
                        var cell = new Cell()
                        {
                            CellReference = GetCellReference(i + 1, 1),
                            DataType = new EnumValue<CellValues>(CellValues.SharedString),
                            CellValue = new CellValue(sstIndex.ToString())
                        };
                        headerRow.Append(cell);
                    }
                    sheetData.InsertAt(headerRow, 0);
                    wsPart.Worksheet.Save();
                    wbPart.Workbook.Save();

                    LogOperation("HEADER_CREATE", "Header row dibuat");
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Gagal memastikan header row: {ex.Message}", ex);
            }
        }

        static WorksheetPart? GetWorksheetPartByName(WorkbookPart workbookPart, string sheetName)
        {
            var sheets = workbookPart.Workbook?.GetFirstChild<Sheets>();
            if (sheets == null) return null;
            var sheet = sheets.Elements<Sheet>().FirstOrDefault(s => string.Equals(s.Name?.Value, sheetName, StringComparison.OrdinalIgnoreCase));
            if (sheet == null) return null;
            var id = sheet.Id?.Value;
            if (string.IsNullOrEmpty(id)) return null;
            return workbookPart.GetPartById(id) as WorksheetPart;
        }

        static string? GetCellText(Cell cell, SharedStringTable? sst)
        {
            // Handle shared strings first
            var dt = cell.DataType?.Value;
            if (dt == CellValues.SharedString)
            {
                if (int.TryParse(cell.CellValue?.InnerText, out int sstIndex) && sst != null)
                {
                    var ssi = sst.Elements<SharedStringItem>().ElementAtOrDefault(sstIndex);
                    // SharedStringItem may contain Text or InnerText
                    return ssi?.InnerText ?? ssi?.Text?.Text;
                }
                return cell.CellValue?.InnerText;
            }

            // Inline string
            if (dt == CellValues.InlineString)
            {
                return cell.InnerText;
            }

            // Otherwise numeric or boolean or formula result
            return cell.CellValue?.InnerText;
        }

        static int InsertSharedStringItem(WorkbookPart wbPart, string text)
        {
            var sstPart = wbPart.GetPartsOfType<SharedStringTablePart>().FirstOrDefault()
             ?? wbPart.AddNewPart<SharedStringTablePart>();

            // Use part's existing root if any; only assign when creating a new SharedStringTable
            var sst = sstPart.SharedStringTable;
            if (sst == null)
            {
                sst = new SharedStringTable();
                sstPart.SharedStringTable = sst;
            }

            // Look for an existing matching entry
            int index = 0;
            foreach (var item in sst.Elements<SharedStringItem>())
            {
                if ((item.Text?.Text ?? item.InnerText) == text)
                    return index;
                index++;
            }

            // Not found -> append
            sst.AppendChild(new SharedStringItem(new Text(text)));
            sst.Save(); // save the root element
            return index;
        }

        static string GetCellReference(int columnIndexZeroBased, int rowIndex)
        {
            int dividend = columnIndexZeroBased + 1;
            string columnName = String.Empty;
            while (dividend > 0)
            {
                int modulo = (dividend - 1) % 26;
                columnName = Convert.ToChar(65 + modulo) + columnName;
                dividend = (dividend - modulo) / 26;
            }
            return $"{columnName}{rowIndex}";
        }

        static List<string[]> ReadRows(string path, string sheetName)
        {
            var result = new List<string[]>();
            try
            {
                ValidateFileAccess(path);

                using var doc = SpreadsheetDocument.Open(path, false);
                var wbPart = doc.WorkbookPart;
                if (wbPart == null)
                    throw new InvalidOperationException("WorkbookPart tidak ditemukan");

                var wsPart = GetWorksheetPartByName(wbPart, sheetName);
                if (wsPart == null)
                    throw new InvalidOperationException($"Worksheet '{sheetName}' tidak ditemukan");

                var sheetData = wsPart.Worksheet.GetFirstChild<SheetData>();
                if (sheetData == null)
                    return result;

                var sst = wbPart.SharedStringTablePart?.SharedStringTable;

                foreach (var row in sheetData.Elements<Row>())
                {
                    var cells = row.Elements<Cell>().ToArray();
                    var values = new List<string>();
                    foreach (var cell in cells)
                    {
                        string? cellText = GetCellText(cell, sst);
                        values.Add(cellText ?? string.Empty);
                    }
                    result.Add(values.ToArray());
                }

                LogOperation("READ_ROWS", $"Berhasil membaca {result.Count} baris dari {sheetName}");
                return result;
            }
            catch (OpenXmlPackageException ex)
            {
                throw new Exception($"Format file Excel tidak valid: {ex.Message}", ex);
            }
            catch (Exception ex)
            {
                throw new Exception($"Gagal membaca baris dari Excel: {ex.Message}", ex);
            }
        }

        static void AddRow(string path, string sheetName, string[] values)
        {
            try
            {
                ValidateFileAccess(path);

                using var doc = SpreadsheetDocument.Open(path, true);
                var wbPart = doc.WorkbookPart ?? throw new InvalidOperationException("WorkbookPart tidak ditemukan");
                var wsPart = GetWorksheetPartByName(wbPart, sheetName) ?? throw new InvalidOperationException("Worksheet tidak ditemukan");
                var sheetData = wsPart.Worksheet.GetFirstChild<SheetData>() ?? wsPart.Worksheet.AppendChild(new SheetData());

                uint nextRowIndex = 1;
                var lastRow = sheetData.Elements<Row>().LastOrDefault();
                if (lastRow != null) nextRowIndex = (lastRow.RowIndex?.Value ?? 0u) + 1u;

                var row = new Row() { RowIndex = nextRowIndex };
                for (int i = 0; i < values.Length; i++)
                {
                    var text = values[i] ?? string.Empty;
                    int sstIndex = InsertSharedStringItem(wbPart, text);

                    var cell = new Cell()
                    {
                        CellReference = GetCellReference(i, (int)nextRowIndex),
                        DataType = new EnumValue<CellValues>(CellValues.SharedString),
                        CellValue = new CellValue(sstIndex.ToString())
                    };
                    row.Append(cell);
                }
                sheetData.Append(row);
                wsPart.Worksheet.Save();
                wbPart.Workbook.Save();

                LogOperation("ADD_ROW", $"Baris ditambahkan ke {sheetName} pada index {nextRowIndex}");
            }
            catch (Exception ex)
            {
                throw new Exception($"Gagal menambah baris: {ex.Message}", ex);
            }
        }

        static bool UpdateRow(string path, string sheetName, int rowIndex, string[] values)
        {
            try
            {
                ValidateFileAccess(path);

                using var doc = SpreadsheetDocument.Open(path, true);
                var wbPart = doc.WorkbookPart;
                if (wbPart == null) return false;

                var wsPart = GetWorksheetPartByName(wbPart, sheetName);
                if (wsPart == null) return false;

                var sheetData = wsPart.Worksheet.GetFirstChild<SheetData>();
                if (sheetData == null) return false;

                var row = sheetData.Elements<Row>().FirstOrDefault(r => (r.RowIndex?.Value ?? 0u) == (uint)rowIndex);
                if (row == null) return false;

                row.RemoveAllChildren<Cell>();
                for (int i = 0; i < values.Length; i++)
                {
                    var text = values[i] ?? string.Empty;
                    int sstIndex = InsertSharedStringItem(wbPart, text);

                    var cell = new Cell()
                    {
                        CellReference = GetCellReference(i, rowIndex),
                        DataType = new EnumValue<CellValues>(CellValues.SharedString),
                        CellValue = new CellValue(sstIndex.ToString())
                    };
                    row.Append(cell);
                }
                wsPart.Worksheet.Save();
                wbPart.Workbook.Save();

                LogOperation("UPDATE_ROW", $"Baris {rowIndex} diperbarui di {sheetName}");
                return true;
            }
            catch (Exception ex)
            {
                throw new Exception($"Gagal memperbarui baris: {ex.Message}", ex);
            }
        }

        static bool DeleteRow(string path, string sheetName, int rowIndex)
        {
            try
            {
                using var doc = SpreadsheetDocument.Open(path, true);
                var wbPart = doc.WorkbookPart;
                if (wbPart == null) return false;
                var wsPart = GetWorksheetPartByName(wbPart, sheetName);
                if (wsPart == null) return false;
                var sheetData = wsPart.Worksheet.GetFirstChild<SheetData>();
                if (sheetData == null) return false;

                var row = sheetData.Elements<Row>().FirstOrDefault(r => (r.RowIndex?.Value ?? 0u) == (uint)rowIndex);
                if (row == null) return false;

                row.Remove();
                wsPart.Worksheet.Save();
                wbPart.Workbook.Save();
                return true;
            }
            catch (Exception ex)
            {
                throw new Exception($"Gagal menghapus baris: {ex.Message}", ex);
            }
        }

        static bool DeleteRowWithSync(string path, string sheetName, int rowIndex)
        {
            // read row to get Id
            var rows = ReadRows(path, sheetName);
            if (rowIndex < 1 || rowIndex > rows.Count) return false;
            var row = rows[rowIndex - 1];
            long id = 0;
            if (row.Length > 0 && long.TryParse(row[0], out var parsed)) id = parsed;

            bool ok = DeleteRow(path, sheetName, rowIndex);
            if (ok && MySqlEnabled && id > 0 && !string.IsNullOrEmpty(MySqlConnectionString) && !string.IsNullOrEmpty(MySqlTableName))
            {
                try
                {
                    DeleteEntryFromMySql(MySqlConnectionString, MySqlTableName, id);
                }
                catch (Exception ex)
                {
                    // Log DB delete error but keep Excel change
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Peringatan MySQL (hapus):\n" + ex.ToString());
                    Console.ResetColor();
                }
            }
            return ok;
        }
        #endregion

        #region Menu Operations
        static void ImportCsvMenu(string filePath, string sheetName)
        {
            try
            {
                Console.Write("Masukkan path file CSV untuk diimpor: ");
                var csvPath = (Console.ReadLine() ?? string.Empty).Trim();
                if (File.Exists(csvPath))
                {
                    int added = ImportCsvAndAddRows(filePath, sheetName, csvPath);
                    ShowSuccess($"Impor selesai. Baris ditambahkan: {added}");
                }
                else
                {
                    ShowError("File CSV tidak ditemukan.");
                }
            }
            catch (Exception ex)
            {
                ShowError($"Gagal import CSV: {ex.Message}");
            }
        }

        static int ImportCsvAndAddRows(string workbookPath, string sheetName, string csvPath)
        {
            var allLines = File.ReadAllLines(csvPath).Select(l => l.Trim()).Where(l => !string.IsNullOrEmpty(l)).ToArray();
            if (allLines.Length == 0) return 0;

            bool firstIsHeader = false;
            var headers = EntryRecord.Headers;
            var firstFields = SplitCsvLine(allLines[0]);
            if (firstFields.Length >= headers.Length && headers.SequenceEqual(firstFields.Take(headers.Length), StringComparer.OrdinalIgnoreCase))
                firstIsHeader = true;

            int startIndex = firstIsHeader ? 1 : 0;
            int added = 0;
            for (int i = startIndex; i < allLines.Length; i++)
            {
                var fields = SplitCsvLine(allLines[i]);
                var rec = ParseEntryFromFields(fields, headers);
                AddEntryRecord(workbookPath, sheetName, rec);
                added++;
            }
            return added;
        }

        static string[] SplitCsvLine(string line)
        {
            if (string.IsNullOrEmpty(line)) return Array.Empty<string>();

            var parts = new List<string>();
            bool inQuotes = false;
            var cur = new System.Text.StringBuilder();
            for (int i = 0; i < line.Length; i++)
            {
                char c = line[i];
                if (c == '"')
                {
                    if (inQuotes && i + 1 < line.Length && line[i + 1] == '"')
                    {
                        // escaped quote
                        cur.Append('"');
                        i++; // skip next quote
                    }
                    else
                    {
                        inQuotes = !inQuotes;
                    }
                    continue;
                }

                if (c == ',' && !inQuotes)
                {
                    parts.Add(cur.ToString().Trim());
                    cur.Clear();
                }
                else
                {
                    cur.Append(c);
                }
            }
            parts.Add(cur.ToString().Trim());

            // Trim surrounding quotes on each field
            for (int i = 0; i < parts.Count; i++)
            {
                var p = parts[i];
                if (p.Length >= 2 && p.StartsWith("\"") && p.EndsWith("\""))
                    p = p.Substring(1, p.Length - 2);
                parts[i] = p;
            }

            return parts.ToArray();
        }

        static void AddRowMenu(string filePath, string sheetName)
        {
            try
            {
                Console.Write("Masukkan nilai dipisah koma untuk ditambahkan: ");
                var values = ReadCsvLine();
                if (values.Length >= EntryRecord.Headers.Length)
                {
                    var rec = ParseEntryFromFields(values, EntryRecord.Headers);
                    ExecuteWithRetry<bool>(
                        () => { AddEntryRecord(filePath, sheetName, rec); return true; },
                        "Add Entry Record",
                        2,
                        1000
                    );
                    ShowSuccess("Baris ditambahkan.");
                }
                else
                {
                    // Non-entry row: prefix empty Id
                    var pref = new string[values.Length + 1];
                    pref[0] = string.Empty;
                    Array.Copy(values, 0, pref, 1, values.Length);
                    ExecuteWithRetry<bool>(
                        () => { AddRow(filePath, sheetName, pref); return true; },
                        "Add Row",
                        2,
                        1000
                    );
                    ShowSuccess("Baris ditambahkan.");
                }
            }
            catch (Exception ex)
            {
                ShowError($"Gagal menambah baris: {ex.Message}");
                LogOperation("ERROR", $"AddRowMenu failed: {ex.Message}");
            }
        }

        static string[] ReadCsvLine()
        {
            var line = Console.ReadLine() ?? string.Empty;
            return SplitCsvLine(line);
        }

        static EntryRecord ParseEntryFromFields(string[] fields, string[] headers)
        {
            // assume order matches headers
            string Get(int idx) => idx >= 0 && idx < fields.Length ? fields[idx] : string.Empty;

            string date = Get(0);
            string shift = Get(1);
            string codeRef = Get(2);
            string machine = Get(3);
            string area = Get(4);
            string autoAdj = Get(5);
            string topTec = Get(6);
            string final = Get(7);
            string packaging = Get(8);
            int qtyIn = TryParseInt(Get(9));
            int qtyGood = TryParseInt(Get(10));
            int qtyBad = TryParseInt(Get(11));
            int reject = TryParseInt(Get(12));

            return new EntryRecord(date, shift, codeRef, machine, area, autoAdj, topTec, final, packaging, qtyIn, qtyGood, qtyBad, reject);
        }

        static int TryParseInt(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return 0;
            if (int.TryParse(s, out var v)) return v;
            var digits = new string(s.Where(c => char.IsDigit(c) || c == '-').ToArray());
            if (int.TryParse(digits, out v)) return v;
            return 0;
        }

        static void AddEntryRecord(string path, string sheetName, EntryRecord rec)
        {
            // Insert to DB first to get id (if enabled), then write row with Id prefix
            long id = 0;
            if (MySqlEnabled && !string.IsNullOrEmpty(MySqlConnectionString) && !string.IsNullOrEmpty(MySqlTableName))
            {
                try
                {
                    id = InsertEntryToMySql(MySqlConnectionString, MySqlTableName, rec);
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Peringatan MySQL (sisip):\n" + ex.ToString());
                    Console.ResetColor();
                    id = 0;
                }
            }

            var values = rec.ToValues();
            var withId = new string[values.Length + 1];
            withId[0] = id > 0 ? id.ToString() : string.Empty;
            Array.Copy(values, 0, withId, 1, values.Length);
            AddRow(path, sheetName, withId);
        }

        static void ShowRowsMenu(string filePath, string sheetName)
        {
            try
            {
                var rows = ReadRows(filePath, sheetName);
                if (!rows.Any())
                {
                    ShowWarning("(tidak ada baris)");
                }
                else
                {
                    PrintRowsTable(rows);
                }
            }
            catch (Exception ex)
            {
                ShowError($"Gagal menampilkan baris: {ex.Message}");
            }
        }

        static void UpdateRowMenu(string filePath, string sheetName)
        {
            try
            {
                Console.Write("Masukkan indeks baris untuk diupdate (1-based): ");
                var idxInput = Console.ReadLine();
                if (!int.TryParse(idxInput, out int idx) || idx < 1)
                {
                    ShowError("Indeks tidak valid.");
                    return;
                }

                Console.Write("Masukkan nilai baru dipisah koma: ");
                var values = ReadCsvLine();
                bool ok;

                if (values.Length >= EntryRecord.Headers.Length)
                {
                    var rec = ParseEntryFromFields(values, EntryRecord.Headers);
                    ok = UpdateEntryRecord(filePath, sheetName, idx, rec);
                }
                else
                {
                    // update non-entry row: prefix empty Id and update
                    var pref = new string[values.Length + 1];
                    pref[0] = string.Empty;
                    Array.Copy(values, 0, pref, 1, values.Length);
                    ok = UpdateRow(filePath, sheetName, idx, pref);
                }

                if (ok)
                    ShowSuccess("Baris diperbarui.");
                else
                    ShowError("Baris tidak ditemukan.");
            }
            catch (Exception ex)
            {
                ShowError($"Gagal update baris: {ex.Message}");
            }
        }

        static bool UpdateEntryRecord(string path, string sheetName, int rowIndex, EntryRecord rec)
        {
            // Read existing row to get Id
            var rows = ReadRows(path, sheetName);
            if (rowIndex < 1 || rowIndex > rows.Count) return false;
            var row = rows[rowIndex - 1];
            long id = 0;
            if (row.Length > 0 && long.TryParse(row[0], out var parsed)) id = parsed;

            // Sync to DB
            if (MySqlEnabled && !string.IsNullOrEmpty(MySqlConnectionString) && !string.IsNullOrEmpty(MySqlTableName))
            {
                try
                {
                    if (id > 0)
                    {
                        UpdateEntryInMySql(MySqlConnectionString, MySqlTableName, id, rec);
                    }
                    else
                    {
                        id = InsertEntryToMySql(MySqlConnectionString, MySqlTableName, rec);
                    }
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Peringatan MySQL (perbarui):\n" + ex.ToString());
                    Console.ResetColor();
                }
            }

            // write back to Excel (Id + fields)
            var values = rec.ToValues();
            var withId = new string[values.Length + 1];
            withId[0] = id > 0 ? id.ToString() : string.Empty;
            Array.Copy(values, 0, withId, 1, values.Length);
            return UpdateRow(path, sheetName, rowIndex, withId);
        }

        static void DeleteRowMenu(string filePath, string sheetName)
        {
            try
            {
                Console.Write("Masukkan indeks baris untuk dihapus (1-based): ");
                var idxInput = Console.ReadLine();
                if (!int.TryParse(idxInput, out int idx) || idx < 1)
                {
                    ShowError("Indeks tidak valid.");
                    return;
                }

                bool ok = DeleteRowWithSync(filePath, sheetName, idx);
                if (ok)
                    ShowSuccess("Baris dihapus.");
                else
                    ShowError("Baris tidak ditemukan.");
            }
            catch (Exception ex)
            {
                ShowError($"Gagal menghapus baris: {ex.Message}");
            }
        }

        static void SearchDataMenu(string filePath, string sheetName)
        {
            try
            {
                Console.Write("Masukkan kata kunci pencarian: ");
                string? keyword = Console.ReadLine();

                if (string.IsNullOrWhiteSpace(keyword))
                {
                    ShowError("Kata kunci tidak boleh kosong.");
                    return;
                }

                Console.WriteLine("Mencari data...");
                var results = SearchData(filePath, sheetName, keyword);

                if (results.Any())
                {
                    ShowSuccess($"Ditemukan {results.Count} hasil pencarian:");
                    PrintRowsTable(results);
                }
                else
                {
                    ShowWarning("Tidak ditemukan data yang sesuai.");
                }
            }
            catch (Exception ex)
            {
                ShowError($"Gagal mencari data: {ex.Message}");
            }
        }

        static List<string[]> SearchData(string filePath, string sheetName, string keyword)
        {
            var allRows = ReadRows(filePath, sheetName);
            var results = new List<string[]>();

            foreach (var row in allRows)
            {
                if (row.Any(cell => cell.Contains(keyword, StringComparison.OrdinalIgnoreCase)))
                {
                    results.Add(row);
                }
            }

            return results;
        }

        static void BatchOperationsMenu(string filePath, string sheetName)
        {
            try
            {
                Console.WriteLine("\n=== Batch Operations ===");
                Console.WriteLine("1. Bulk Insert from CSV");
                Console.WriteLine("2. Delete All Data");
                Console.WriteLine("3. Sync Excel to MySQL");
                Console.WriteLine("4. Sync MySQL to Excel");
                Console.Write("Pilihan (1-4): ");

                var choice = Console.ReadLine();
                switch (choice)
                {
                    case "1":
                        BulkInsertFromCsv(filePath, sheetName);
                        break;
                    case "2":
                        DeleteAllData(filePath, sheetName);
                        break;
                    case "3":
                        SyncExcelToMySQL(filePath, sheetName);
                        break;
                    case "4":
                        SyncMySQLToExcel(filePath, sheetName);
                        break;
                    default:
                        ShowError("Pilihan tidak valid.");
                        break;
                }
            }
            catch (Exception ex)
            {
                ShowError($"Gagal operasi batch: {ex.Message}");
            }
        }

        static void BulkInsertFromCsv(string filePath, string sheetName)
        {
            try
            {
                Console.Write("Masukkan path file CSV: ");
                string? csvPath = Console.ReadLine();

                if (string.IsNullOrWhiteSpace(csvPath) || !File.Exists(csvPath))
                {
                    ShowError("File CSV tidak ditemukan.");
                    return;
                }

                Console.Write("Apakah yakin melakukan bulk insert? (Y/N): ");
                if (Console.ReadLine()?.Trim().ToUpper() != "Y") return;

                int successCount = 0;
                int errorCount = 0;

                var lines = File.ReadAllLines(csvPath);
                foreach (var line in lines.Skip(1)) // Skip header
                {
                    try
                    {
                        var values = SplitCsvLine(line);
                        if (values.Length >= EntryRecord.Headers.Length)
                        {
                            var rec = ParseEntryFromFields(values, EntryRecord.Headers);
                            AddEntryRecord(filePath, sheetName, rec);
                            successCount++;
                        }
                    }
                    catch
                    {
                        errorCount++;
                    }
                }

                ShowSuccess($"Bulk insert selesai. Berhasil: {successCount}, Gagal: {errorCount}");
            }
            catch (Exception ex)
            {
                ShowError($"Gagal bulk insert: {ex.Message}");
            }
        }

        static void DeleteAllData(string filePath, string sheetName)
        {
            try
            {
                Console.Write("APAKAH ANDA YAKIN INGIN MENGHAPUS SEMUA DATA? (KETIK 'HAPUS SEMUA'): ");
                if (Console.ReadLine()?.Trim() != "HAPUS SEMUA")
                {
                    ShowWarning("Operasi dibatalkan.");
                    return;
                }

                var rows = ReadRows(filePath, sheetName);
                for (int i = rows.Count - 1; i >= 1; i--) // Keep header
                {
                    DeleteRowWithSync(filePath, sheetName, i + 1);
                }

                ShowSuccess("Semua data berhasil dihapus.");
            }
            catch (Exception ex)
            {
                ShowError($"Gagal menghapus semua data: {ex.Message}");
            }
        }

        static void SyncExcelToMySQL(string filePath, string sheetName)
        {
            try
            {
                if (!MySqlEnabled)
                {
                    ShowError("MySQL tidak aktif.");
                    return;
                }

                var rows = ReadRows(filePath, sheetName);
                int syncedCount = 0;

                for (int i = 1; i < rows.Count; i++) // Skip header
                {
                    try
                    {
                        var row = rows[i];
                        if (row.Length >= EntryRecord.Headers.Length + 1)
                        {
                            var rec = ParseEntryFromFields(row.Skip(1).ToArray(), EntryRecord.Headers);
                            long id = InsertEntryToMySql(MySqlConnectionString!, MySqlTableName!, rec);
                            syncedCount++;
                        }
                    }
                    catch
                    {
                        // Continue with next row
                    }
                }

                ShowSuccess($"Sinkronisasi selesai. {syncedCount} data disinkronisasi ke MySQL.");
            }
            catch (Exception ex)
            {
                ShowError($"Gagal sinkronisasi Excel ke MySQL: {ex.Message}");
            }
        }

        static void SyncMySQLToExcel(string filePath, string sheetName)
        {
            ExportMySqlToExcel(filePath, sheetName);
        }

        static void LiveMonitor(string filePath, string sheetName)
        {
            try
            {
                // Monitor the workbook file and refresh the console display on changes.
                string? folder = Path.GetDirectoryName(filePath);
                string file = Path.GetFileName(filePath);
                folder = string.IsNullOrEmpty(folder) ? Directory.GetCurrentDirectory() : folder;

                using var watcher = new FileSystemWatcher(folder, file);
                watcher.NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.Size | NotifyFilters.FileName;

                bool needRefresh = true;

                FileSystemEventHandler onChange = (s, e) => needRefresh = true;
                RenamedEventHandler onRenamed = (s, e) => needRefresh = true;

                watcher.Changed += onChange;
                watcher.Created += onChange;
                watcher.Renamed += onRenamed;
                watcher.EnableRaisingEvents = true;

                try
                {
                    DrawTitle($"Monitor langsung - {file} : {sheetName} (tekan Q untuk berhenti)");
                    while (true)
                    {
                        if (needRefresh)
                        {
                            try
                            {
                                var rows = ReadRows(filePath, sheetName);
                                Console.Clear();
                                DrawTitle($"Monitor langsung - {file} : {sheetName} (tekan Q untuk berhenti)");
                                if (!rows.Any())
                                {
                                    Console.ForegroundColor = ConsoleColor.DarkGray;
                                    Console.WriteLine("(tidak ada baris)");
                                    Console.ResetColor();
                                }
                                else
                                {
                                    PrintRowsTable(rows);
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.ForegroundColor = ConsoleColor.Red;
                                Console.WriteLine("Kesalahan membaca file: " + ex.Message);
                                Console.ResetColor();
                            }
                            needRefresh = false;
                        }

                        if (Console.KeyAvailable)
                        {
                            var key = Console.ReadKey(true);
                            if (key.Key == ConsoleKey.Q)
                            {
                                Console.WriteLine("Menghentikan monitor...");
                                break;
                            }
                        }

                        Thread.Sleep(300);
                    }
                }
                finally
                {
                    watcher.EnableRaisingEvents = false;
                }
            }
            catch (Exception ex)
            {
                ShowError($"Gagal monitor langsung: {ex.Message}");
            }
        }

        static void ShowConfigurationMenu()
        {
            try
            {
                Console.WriteLine("\n=== Configuration ===");
                Console.WriteLine($"1. Default File Path: {config.DefaultFilePath}");
                Console.WriteLine($"2. Default Sheet Name: {config.DefaultSheetName}");
                Console.WriteLine($"3. Auto Backup: {config.AutoBackup}");
                Console.WriteLine($"4. Backup Retention Days: {config.BackupRetentionDays}");
                Console.WriteLine($"5. Reset Configuration");
                Console.WriteLine($"6. Save Configuration");
                Console.Write("Pilihan (1-6): ");

                var choice = Console.ReadLine();
                switch (choice)
                {
                    case "1":
                        Console.Write("New default file path: ");
                        config.DefaultFilePath = Console.ReadLine();
                        break;
                    case "2":
                        Console.Write("New default sheet name: ");
                        config.DefaultSheetName = Console.ReadLine();
                        break;
                    case "3":
                        config.AutoBackup = !config.AutoBackup;
                        Console.WriteLine($"Auto Backup: {config.AutoBackup}");
                        break;
                    case "4":
                        Console.Write("Backup retention days: ");
                        if (int.TryParse(Console.ReadLine(), out int days))
                            config.BackupRetentionDays = days;
                        break;
                    case "5":
                        config = new AppConfig();
                        Console.WriteLine("Configuration reset.");
                        break;
                    case "6":
                        config.Save();
                        ShowSuccess("Configuration saved.");
                        break;
                }
            }
            catch (Exception ex)
            {
                ShowError($"Gagal konfigurasi: {ex.Message}");
            }
        }

        static void ShowLogsMenu()
        {
            try
            {
                Console.WriteLine("\n=== Operation Logs ===");
                Console.WriteLine($"Total operations: {operationLogs.Count}");

                var recentLogs = operationLogs.TakeLast(20).ToList();
                foreach (var log in recentLogs)
                {
                    Console.WriteLine($"{log.Timestamp:HH:mm:ss} [{log.OperationType}] {log.Description}");
                }

                Console.WriteLine("\n1. Clear Logs");
                Console.WriteLine("2. Export Logs");
                Console.Write("Pilihan (1-2): ");

                var choice = Console.ReadLine();
                if (choice == "1")
                {
                    operationLogs.Clear();
                    ShowSuccess("Logs cleared.");
                }
                else if (choice == "2")
                {
                    ExportLogs();
                }
            }
            catch (Exception ex)
            {
                ShowError($"Gagal menampilkan logs: {ex.Message}");
            }
        }

        static void ExportLogs()
        {
            try
            {
                string exportPath = $"operation_logs_{DateTime.Now:yyyyMMdd_HHmmss}.txt";
                var logContent = operationLogs.Select(log =>
                    $"{log.Timestamp:yyyy-MM-dd HH:mm:ss} [{log.OperationType}] {log.Description}"
                );

                File.WriteAllLines(exportPath, logContent);
                ShowSuccess($"Logs exported to: {exportPath}");
            }
            catch (Exception ex)
            {
                ShowError($"Gagal export logs: {ex.Message}");
            }
        }
        #endregion

        #region MySQL Operations
        static void ShowMySqlConfigurationMenu(string sheetName)
        {
            try
            {
                Console.WriteLine("\n=== Konfigurasi MySQL ===");
                Console.WriteLine("Pilih metode koneksi:");
                Console.WriteLine("1. Auto-Connect (Recommended)");
                Console.WriteLine("2. Manual Configuration");
                Console.WriteLine("3. Quick Local Setup");
                Console.WriteLine("4. Test Current Connection");
                Console.Write("Pilihan (1-4): ");
                var choice = Console.ReadLine()?.Trim();

                switch (choice)
                {
                    case "1": AutoConnectToMySQL(sheetName); break;
                    case "2": ConfigureMySQL(sheetName); break;
                    case "3": QuickLocalSetup(sheetName); break;
                    case "4": TestMySqlConnection(); break;
                    default:
                        ShowError("Pilihan tidak valid.");
                        break;
                }
            }
            catch (Exception ex)
            {
                ShowError($"Gagal konfigurasi MySQL: {ex.Message}");
            }
        }

        static void ConfigureMySQL(string sheetName)
        {
            try
            {
                Console.WriteLine("\n=== Konfigurasi MySQL ===");
                Console.WriteLine("Pilih metode koneksi:");
                Console.WriteLine("1. Auto-Connect (Recommended)");
                Console.WriteLine("2. Manual Configuration");
                Console.WriteLine("3. Quick Local Setup");
                Console.WriteLine("4. Test Current Connection");
                Console.WriteLine("5. Skip MySQL Configuration");
                Console.Write("Pilihan (1-5): ");
                var choice = Console.ReadLine()?.Trim();

                switch (choice)
                {
                    case "1":
                        AutoConnectToMySQL(sheetName);
                        break;
                    case "2":
                        ManualMySQLConfiguration(sheetName);
                        break;
                    case "3":
                        QuickLocalSetup(sheetName);
                        break;
                    case "4":
                        TestMySqlConnection();
                        break;
                    case "5":
                        ShowInfo("Konfigurasi MySQL dilewati");
                        break;
                    default:
                        ShowError("Pilihan tidak valid.");
                        break;
                }
            }
            catch (Exception ex)
            {
                LogOperation("ERROR", $"Konfigurasi MySQL gagal: {ex.Message}");
                ShowError($"Konfigurasi MySQL gagal: {ex.Message}");
            }
        }

        static void ManualMySQLConfiguration(string sheetName)
        {
            try
            {
                Console.WriteLine("\nMasukkan informasi server MySQL:");
                Console.Write("Server (default: localhost): ");
                string server = (Console.ReadLine() ?? "localhost").Trim();
                if (string.IsNullOrEmpty(server)) server = "localhost";

                Console.Write("Port (default: 3306): ");
                string portInput = (Console.ReadLine() ?? "3306").Trim();
                if (!int.TryParse(portInput, out int port) || port <= 0)
                {
                    port = 3306;
                    ShowWarning($"Port tidak valid, menggunakan default: {port}");
                }

                Console.Write("Username (default: root): ");
                string username = (Console.ReadLine() ?? "root").Trim();
                if (string.IsNullOrEmpty(username)) username = "root";

                Console.Write("Password: ");
                string password = (Console.ReadLine() ?? "").Trim();

                string baseConnStr = $"server={server};port={port};user={username};password={password};";

                // Test connection
                TestMySqlConnection(baseConnStr, "Koneksi manual");

                // Get or create database
                Console.Write("Nama database (default: batam_sed_db): ");
                MySqlDatabaseName = (Console.ReadLine() ?? "batam_sed_db").Trim();
                if (string.IsNullOrEmpty(MySqlDatabaseName)) MySqlDatabaseName = "batam_sed_db";

                // Create database if not exists
                CreateDatabaseIfNotExists(baseConnStr, MySqlDatabaseName);

                // Now create full connection string with database
                MySqlConnectionString = $"server={server};port={port};database={MySqlDatabaseName};user={username};password={password};";

                // Get table name
                Console.Write($"Nama tabel (default: {SanitizeTableName(sheetName)}): ");
                var tbl = (Console.ReadLine() ?? string.Empty).Trim();
                MySqlTableName = string.IsNullOrEmpty(tbl) ? SanitizeTableName(sheetName) : SanitizeTableName(tbl);

                // Ensure table exists
                EnsureMySqlTable(MySqlConnectionString, MySqlTableName);
                MySqlEnabled = true;

                ShowSuccess($"✓ MySQL diaktifkan. Database: {MySqlDatabaseName}, Tabel: {MySqlTableName}");
                LogOperation("MYSQL_CONFIG", $"MySQL manual configured: {server}:{port}, DB: {MySqlDatabaseName}");
            }
            catch (Exception ex)
            {
                MySqlEnabled = false;
                throw new Exception($"Konfigurasi manual MySQL gagal: {ex.Message}", ex);
            }
        }

        static void AutoConnectToMySQL(string sheetName)
        {
            Console.WriteLine("\n=== MySQL Auto-Connect ===");
            Console.WriteLine("Mencoba terhubung ke MySQL service secara otomatis...");

            // Coba beberapa konfigurasi umum MySQL
            var commonConfigs = new[]
            {
               new { Server = "localhost", Port = 3306, User = "root", Password = "" },
               new { Server = "127.0.0.1", Port = 3306, User = "root", Password = "" },
               new { Server = "localhost", Port = 3306, User = "root", Password = "root" },
               new { Server = "localhost", Port = 3307, User = "root", Password = "" },
               new { Server = "localhost", Port = 3306, User = "admin", Password = "admin" },
           };

            foreach (var config in commonConfigs)
            {
                string testConnStr = $"server={config.Server};port={config.Port};user={config.User};password={config.Password};";

                Console.Write($"Mencoba {config.User}@{config.Server}:{config.Port}... ");

                try
                {
                    using var testConn = new MySqlConnection(testConnStr);
                    testConn.Open();

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("✓ BERHASIL");
                    Console.ResetColor();

                    // Gunakan konfigurasi ini
                    UseDiscoveredMySQL(config.Server, config.Port, config.User, config.Password, sheetName);
                    return;
                }
                catch
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("✗ GAGAL");
                    Console.ResetColor();
                }
            }

            // Jika semua gagal, coba start MySQL service
            Console.WriteLine("\nMencoba memulai MySQL Service...");
            if (TryStartMySQLService())
            {
                // Tunggu sebentar lalu coba lagi
                Thread.Sleep(3000);
                AutoConnectToMySQL(sheetName);
                return;
            }

            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("\nTidak dapat terhubung ke MySQL secara otomatis.");
            Console.WriteLine("Gunakan opsi Settings MySQL untuk konfigurasi manual.");
            Console.ResetColor();
        }

        static void QuickLocalSetup(string sheetName)
        {
            Console.WriteLine("\n=== Quick Local MySQL Setup ===");
            Console.WriteLine("Mencoba setup MySQL lokal secara otomatis...");

            string[] commonServers = { "localhost", "127.0.0.1", "::1" };
            int[] commonPorts = { 3306, 3307, 3308 };
            string[] commonUsers = { "root", "admin", "user" };
            string[] commonPasswords = { "", "root", "admin", "password", "123456" };

            bool connected = false;

            foreach (var server in commonServers)
            {
                foreach (var port in commonPorts)
                {
                    foreach (var user in commonUsers)
                    {
                        foreach (var password in commonPasswords)
                        {
                            string testConnStr = $"server={server};port={port};user={user};password={password};";

                            Console.Write($"Mencoba {user}@{server}:{port}... ");

                            try
                            {
                                using var testConn = new MySqlConnection(testConnStr);
                                testConn.Open();

                                Console.ForegroundColor = ConsoleColor.Green;
                                Console.WriteLine("✓ BERHASIL");
                                Console.ResetColor();

                                // Setup database dan tabel
                                MySqlDatabaseName = "batam_sed_db";
                                MySqlConnectionString = $"server={server};port={port};database={MySqlDatabaseName};user={user};password={password};";
                                MySqlTableName = SanitizeTableName(sheetName);

                                CreateDatabaseIfNotExists(testConnStr, MySqlDatabaseName);
                                EnsureMySqlTable(MySqlConnectionString, MySqlTableName);

                                MySqlEnabled = true;
                                connected = true;

                                Console.ForegroundColor = ConsoleColor.Green;
                                Console.WriteLine($"✓ Quick Setup BERHASIL!");
                                Console.WriteLine($"  Database: {MySqlDatabaseName}");
                                Console.WriteLine($"  Tabel: {MySqlTableName}");
                                Console.ResetColor();

                                return;
                            }
                            catch
                            {
                                Console.ForegroundColor = ConsoleColor.Red;
                                Console.WriteLine("✗ GAGAL");
                                Console.ResetColor();
                            }
                        }
                    }
                }
            }

            if (!connected)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("✗ Tidak dapat terhubung ke MySQL lokal.");
                Console.WriteLine("  Pastikan MySQL server sudah terinstall dan running.");
                Console.ResetColor();
            }
        }

        static bool TryStartMySQLService()
        {
            try
            {
                Console.WriteLine("Mencoba memulai MySQL service...");

                // Coba berbagai command untuk start MySQL
                string[] commands = {
                    "net start mysql",
                    "net start mysql80",
                    "net start mysql57",
                    "net start mariadb",
                    "sc start mysql",
                    "systemctl start mysql",
                    "systemctl start mariadb"
                };

                foreach (var cmd in commands)
                {
                    Console.Write($"Mencoba: {cmd}... ");

                    try
                    {
                        var process = new Process
                        {
                            StartInfo = new ProcessStartInfo
                            {
                                FileName = "cmd.exe",
                                Arguments = $"/c {cmd}",
                                UseShellExecute = false,
                                CreateNoWindow = true,
                                RedirectStandardOutput = true,
                                RedirectStandardError = true
                            }
                        };

                        process.Start();
                        string output = process.StandardOutput.ReadToEnd();
                        string error = process.StandardError.ReadToEnd();
                        process.WaitForExit(5000);

                        if (process.ExitCode == 0 || output.ToLower().Contains("started") || output.ToLower().Contains("running"))
                        {
                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine("✓ BERHASIL");
                            Console.ResetColor();
                            Thread.Sleep(2000); // Tunggu service fully started
                            return true;
                        }
                        else
                        {
                            Console.ForegroundColor = ConsoleColor.Red;
                            Console.WriteLine("✗ Gagal");
                            Console.ResetColor();
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"Error: {ex.Message}");
                        Console.ResetColor();
                    }
                }

                // Jika semua command gagal, coba langsung koneksi (mungkin service sudah running)
                Console.WriteLine("Mencoba koneksi langsung...");
                return true;
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Error start service: {ex.Message}");
                Console.ResetColor();
                return false;
            }
        }

        static void UseDiscoveredMySQL(string server, int port, string user, string password, string sheetName)
        {
            try
            {
                string baseConnStr = $"server={server};port={port};user={user};password={password};";

                // Get or create database
                Console.Write("Nama database (default: batam_sed_db): ");
                MySqlDatabaseName = (Console.ReadLine() ?? "batam_sed_db").Trim();
                if (string.IsNullOrEmpty(MySqlDatabaseName)) MySqlDatabaseName = "batam_sed_db";

                // Create database if not exists
                CreateDatabaseIfNotExists(baseConnStr, MySqlDatabaseName);

                // Now create full connection string with database
                MySqlConnectionString = $"server={server};port={port};database={MySqlDatabaseName};user={user};password={password};";

                // Get table name
                Console.Write($"Nama tabel (default: {SanitizeTableName(sheetName)}): ");
                var tbl = (Console.ReadLine() ?? string.Empty).Trim();
                MySqlTableName = string.IsNullOrEmpty(tbl) ? SanitizeTableName(sheetName) : SanitizeTableName(tbl);

                // Ensure table exists
                EnsureMySqlTable(MySqlConnectionString, MySqlTableName);
                MySqlEnabled = true;

                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"✓ Auto-Connect BERHASIL!");
                Console.WriteLine($"  Database: {MySqlDatabaseName}");
                Console.WriteLine($"  Tabel: {MySqlTableName}");
                Console.WriteLine($"  Server: {server}:{port}");
                Console.ResetColor();
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"✗ Auto-Connect GAGAL: {ex.Message}");
                Console.ResetColor();
            }
        }

        static void CreateDatabaseIfNotExists(string baseConnStr, string databaseName)
        {
            try
            {
                using var conn = new MySqlConnection(baseConnStr);
                conn.Open();

                using var cmd = conn.CreateCommand();
                cmd.CommandText = $"CREATE DATABASE IF NOT EXISTS `{databaseName}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;";
                cmd.ExecuteNonQuery();

                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"✓ Database '{databaseName}' siap digunakan");
                Console.ResetColor();
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"⚠ Peringatan: Gagal membuat database '{databaseName}': {ex.Message}");
                Console.WriteLine("  Melanjutkan dengan database yang sudah ada...");
                Console.ResetColor();
            }
        }

        static string SanitizeTableName(string raw)
        {
            if (string.IsNullOrEmpty(raw)) return "sheet_data";
            var t = new string(raw.Where(c => char.IsLetterOrDigit(c) || c == '_').ToArray());
            if (string.IsNullOrEmpty(t)) return "sheet_data";
            return t.Length > 64 ? t.Substring(0, 64) : t;
        }

        static void TestMySqlConnection(string? connectionString = null, string context = "Koneksi saat ini")
        {
            string connStr = connectionString ?? MySqlConnectionString ?? "";

            if (string.IsNullOrEmpty(connStr))
            {
                ShowError("String koneksi MySQL tidak ditemukan.");
                return;
            }

            try
            {
                using var conn = new MySqlConnection(connStr);
                conn.Open();

                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"✓ {context} BERHASIL");

                // Get server info
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT VERSION() as version, NOW() as time, DATABASE() as db";
                using var reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    Console.WriteLine($"  MySQL Version: {reader["version"]}");
                    Console.WriteLine($"  Server Time: {reader["time"]}");
                    Console.WriteLine($"  Database: {reader["db"] ?? "N/A"}");
                }
                Console.ResetColor();

                LogOperation("MYSQL_TEST", $"{context} berhasil");
            }
            catch (MySqlException ex)
            {
                throw new Exception($"Koneksi MySQL gagal (Error {ex.Number}): {ex.Message}", ex);
            }
            catch (Exception ex)
            {
                throw new Exception($"Test koneksi MySQL gagal: {ex.Message}", ex);
            }
        }

        static void EnsureMySqlTable(string? connStr, string? tableName)
        {
            if (string.IsNullOrEmpty(connStr) || string.IsNullOrEmpty(tableName))
                throw new ArgumentException("Connection string atau nama tabel hilang.");

            try
            {
                using var conn = new MySqlConnection(connStr);
                conn.Open();

                using var cmd = conn.CreateCommand();
                cmd.CommandText = $@"
CREATE TABLE IF NOT EXISTS `{tableName}` (
    `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `Date` DATE NULL,
    `Shift` VARCHAR(32) NULL,
    `CodeReference` VARCHAR(128) NULL,
    `MachineNumber` VARCHAR(64) NULL,
    `Area` VARCHAR(64) NULL,
    `AutoAdjustment` VARCHAR(128) NULL,
    `TopTec` VARCHAR(128) NULL,
    `FinalTester` VARCHAR(128) NULL,
    `Packaging` VARCHAR(128) NULL,
    `QuantityInput` INT NULL,
    `QuantityGood` INT NULL,
    `QuantityBad` INT NULL,
    `Reject` INT NULL,
    `CreatedAt` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;";
                cmd.ExecuteNonQuery();

                ShowSuccess($"✓ Tabel '{tableName}' siap digunakan");
                LogOperation("MYSQL_TABLE", $"Tabel dipastikan: {tableName}");
            }
            catch (MySqlException ex)
            {
                throw new Exception($"Gagal membuat tabel MySQL (Error {ex.Number}): {ex.Message}", ex);
            }
        }

        static long InsertEntryToMySql(string connStr, string tableName, EntryRecord rec)
        {
            using var conn = new MySqlConnection(connStr);
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $@"
INSERT INTO `{tableName}` 
(`Date`,`Shift`,`CodeReference`,`MachineNumber`,`Area`,`AutoAdjustment`,`TopTec`,`FinalTester`,`Packaging`,`QuantityInput`,`QuantityGood`,`QuantityBad`,`Reject`)
VALUES (@Date,@Shift,@CodeReference,@MachineNumber,@Area,@AutoAdjustment,@TopTec,@FinalTester,@Packaging,@QuantityInput,@QuantityGood,@QuantityBad,@Reject);";

            DateTime dt;
            if (TryGetDate(rec.Date, out dt))
                cmd.Parameters.AddWithValue("@Date", dt.Date);
            else
                cmd.Parameters.AddWithValue("@Date", DBNull.Value);

            cmd.Parameters.AddWithValue("@Shift", (object?)rec.Shift ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@CodeReference", (object?)rec.CodeReference ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@MachineNumber", (object?)rec.MachineNumber ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@Area", (object?)rec.Area ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@AutoAdjustment", (object?)rec.ProcessAutoAdjustment ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@TopTec", (object?)rec.ProcessTopTec ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@FinalTester", (object?)rec.ProcessFinalTester ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@Packaging", (object?)rec.ProcessPackaging ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@QuantityInput", rec.QuantityInput);
            cmd.Parameters.AddWithValue("@QuantityGood", rec.QuantityGood);
            cmd.Parameters.AddWithValue("@QuantityBad", rec.QuantityBad);
            cmd.Parameters.AddWithValue("@Reject", rec.Reject);

            cmd.ExecuteNonQuery();
            // retrieve last insert id
            using var cmdId = conn.CreateCommand();
            cmdId.CommandText = "SELECT LAST_INSERT_ID();";
            var obj = cmdId.ExecuteScalar();
            if (obj != null && long.TryParse(obj.ToString(), out var id)) return id;
            return 0;
        }

        static bool UpdateEntryInMySql(string connStr, string tableName, long id, EntryRecord rec)
        {
            using var conn = new MySqlConnection(connStr);
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $@"
UPDATE `{tableName}` SET
`Date`=@Date, `Shift`=@Shift, `CodeReference`=@CodeReference, `MachineNumber`=@MachineNumber,
`Area`=@Area, `AutoAdjustment`=@AutoAdjustment, `TopTec`=@TopTec, `FinalTester`=@FinalTester,
`Packaging`=@Packaging, `QuantityInput`=@QuantityInput, `QuantityGood`=@QuantityGood, `QuantityBad`=@QuantityBad, `Reject`=@Reject
WHERE `id`=@id;";

            DateTime dt;
            if (TryGetDate(rec.Date, out dt))
                cmd.Parameters.AddWithValue("@Date", dt.Date);
            else
                cmd.Parameters.AddWithValue("@Date", DBNull.Value);

            cmd.Parameters.AddWithValue("@Shift", (object?)rec.Shift ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@CodeReference", (object?)rec.CodeReference ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@MachineNumber", (object?)rec.MachineNumber ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@Area", (object?)rec.Area ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@AutoAdjustment", (object?)rec.ProcessAutoAdjustment ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@TopTec", (object?)rec.ProcessTopTec ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@FinalTester", (object?)rec.ProcessFinalTester ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@Packaging", (object?)rec.ProcessPackaging ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@QuantityInput", rec.QuantityInput);
            cmd.Parameters.AddWithValue("@QuantityGood", rec.QuantityGood);
            cmd.Parameters.AddWithValue("@QuantityBad", rec.QuantityBad);
            cmd.Parameters.AddWithValue("@Reject", rec.Reject);
            cmd.Parameters.AddWithValue("@id", id);

            var rows = cmd.ExecuteNonQuery();
            return rows > 0;
        }

        static bool DeleteEntryFromMySql(string connStr, string tableName, long id)
        {
            using var conn = new MySqlConnection(connStr);
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"DELETE FROM `{tableName}` WHERE `id`=@id;";
            cmd.Parameters.AddWithValue("@id", id);
            var rows = cmd.ExecuteNonQuery();
            return rows > 0;
        }

        static bool TryGetDate(string? input, out DateTime dt)
        {
            dt = default;
            if (string.IsNullOrWhiteSpace(input)) return false;
            var formats = new[] { "yyyy-MM-dd", "yyyy-M-d", "yyyy/MM/dd", "dd.MM.yyyy", "d.M.yyyy", "M/d/yyyy", "MM/dd/yyyy" };
            if (DateTime.TryParseExact(input.Trim(), formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out dt)) return true;
            if (DateTime.TryParse(input.Trim(), CultureInfo.InvariantCulture, DateTimeStyles.None, out dt)) return true;
            if (DateTime.TryParse(input.Trim(), CultureInfo.CurrentCulture, DateTimeStyles.None, out dt)) return true;
            return false;
        }

        static void ShowMySqlSystemInfo()
        {
            try
            {
                if (!MySqlEnabled || string.IsNullOrEmpty(MySqlConnectionString))
                {
                    ShowError("MySQL belum dikonfigurasi.");
                    return;
                }

                using var conn = new MySqlConnection(MySqlConnectionString);
                conn.Open();

                Console.WriteLine("\n=== MySQL System Information ===");

                // Table information
                using var cmdTables = conn.CreateCommand();
                cmdTables.CommandText = $@"
                    SELECT TABLE_NAME, TABLE_ROWS, CREATE_TIME, UPDATE_TIME 
                    FROM information_schema.TABLES 
                    WHERE TABLE_SCHEMA = DATABASE()";

                using var readerTables = cmdTables.ExecuteReader();
                Console.WriteLine("\n📊 Tables in Database:");
                while (readerTables.Read())
                {
                    Console.WriteLine($"  📋 {readerTables["TABLE_NAME"]} - {readerTables["TABLE_ROWS"]} rows");
                }
                readerTables.Close();

                // Current table stats
                if (!string.IsNullOrEmpty(MySqlTableName))
                {
                    using var cmdStats = conn.CreateCommand();
                    cmdStats.CommandText = $"SELECT COUNT(*) as total_rows FROM `{MySqlTableName}`";
                    var totalRows = cmdStats.ExecuteScalar();
                    Console.WriteLine($"\n📈 Table {MySqlTableName}: {totalRows} total rows");

                    // Column information
                    cmdStats.CommandText = $@"
                        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
                        FROM information_schema.COLUMNS 
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{MySqlTableName}'
                        ORDER BY ORDINAL_POSITION";

                    using var readerColumns = cmdStats.ExecuteReader();
                    Console.WriteLine("  🗂️ Columns:");
                    while (readerColumns.Read())
                    {
                        Console.WriteLine($"    • {readerColumns["COLUMN_NAME"]} ({readerColumns["DATA_TYPE"]})");
                    }
                }

                ShowSuccess("System info retrieved successfully");
            }
            catch (Exception ex)
            {
                ShowError($"Error getting system info: {ex.Message}");
            }
        }

        static void BackupMySqlData()
        {
            try
            {
                if (!MySqlEnabled || string.IsNullOrEmpty(MySqlConnectionString) || string.IsNullOrEmpty(MySqlTableName))
                {
                    ShowError("MySQL belum dikonfigurasi.");
                    return;
                }

                string backupDir = "MySQL_Backups";
                Directory.CreateDirectory(backupDir);
                string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                string backupFile = Path.Combine(backupDir, $"{MySqlTableName}_backup_{timestamp}.sql");

                using var conn = new MySqlConnection(MySqlConnectionString);
                conn.Open();

                using var cmd = conn.CreateCommand();
                cmd.CommandText = $"SELECT * FROM `{MySqlTableName}`";

                using var reader = cmd.ExecuteReader();
                var backupLines = new List<string>();

                // Get column names
                var columns = new List<string>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    columns.Add(reader.GetName(i));
                }

                backupLines.Add($"-- MySQL Backup for table {MySqlTableName}");
                backupLines.Add($"-- Backup time: {DateTime.Now}");
                backupLines.Add($"");
                backupLines.Add($"CREATE TABLE IF NOT EXISTS `{MySqlTableName}` (");
                backupLines.Add($"  `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,");
                backupLines.Add($"  `Date` DATE NULL,");
                backupLines.Add($"  `Shift` VARCHAR(32) NULL,");
                backupLines.Add($"  `CodeReference` VARCHAR(128) NULL,");
                backupLines.Add($"  `MachineNumber` VARCHAR(64) NULL,");
                backupLines.Add($"  `Area` VARCHAR(64) NULL,");
                backupLines.Add($"  `AutoAdjustment` VARCHAR(128) NULL,");
                backupLines.Add($"  `TopTec` VARCHAR(128) NULL,");
                backupLines.Add($"  `FinalTester` VARCHAR(128) NULL,");
                backupLines.Add($"  `Packaging` VARCHAR(128) NULL,");
                backupLines.Add($"  `QuantityInput` INT NULL,");
                backupLines.Add($"  `QuantityGood` INT NULL,");
                backupLines.Add($"  `QuantityBad` INT NULL,");
                backupLines.Add($"  `Reject` INT NULL,");
                backupLines.Add($"  `CreatedAt` TIMESTAMP DEFAULT CURRENT_TIMESTAMP");
                backupLines.Add($");");
                backupLines.Add($"");

                while (reader.Read())
                {
                    var values = new List<string>();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        var value = reader[i];
                        if (value == DBNull.Value)
                        {
                            values.Add("NULL");
                        }
                        else if (value is string || value is DateTime)
                        {
                            values.Add($"'{value.ToString()?.Replace("'", "''")}'");
                        }
                        else
                        {
                            values.Add(value.ToString() ?? "NULL");
                        }
                    }

                    string insertSQL = $"INSERT INTO `{MySqlTableName}` ({string.Join(", ", columns.Select(c => $"`{c}`"))}) VALUES ({string.Join(", ", values)});";
                    backupLines.Add(insertSQL);
                }

                File.WriteAllLines(backupFile, backupLines);

                ShowSuccess($"Backup berhasil disimpan di: {backupFile}");
            }
            catch (Exception ex)
            {
                ShowError($"Backup GAGAL: {ex.Message}");
            }
        }

        static void RestoreMySqlData()
        {
            try
            {
                if (!MySqlEnabled || string.IsNullOrEmpty(MySqlConnectionString) || string.IsNullOrEmpty(MySqlTableName))
                {
                    ShowError("MySQL belum dikonfigurasi.");
                    return;
                }

                string backupDir = "MySQL_Backups";
                if (!Directory.Exists(backupDir))
                {
                    ShowError("Folder backup tidak ditemukan.");
                    return;
                }

                var backupFiles = Directory.GetFiles(backupDir, "*.sql")
                    .Select(f => new FileInfo(f))
                    .OrderByDescending(f => f.LastWriteTime)
                    .ToArray();

                if (backupFiles.Length == 0)
                {
                    ShowError("Tidak ada file backup ditemukan.");
                    return;
                }

                Console.WriteLine("\n=== Pilih File Backup ===");
                for (int i = 0; i < backupFiles.Length; i++)
                {
                    Console.WriteLine($"{i + 1}. {backupFiles[i].Name} ({backupFiles[i].LastWriteTime})");
                }

                Console.Write("Pilih file (1-{0}): ", backupFiles.Length);
                if (int.TryParse(Console.ReadLine(), out int choice) && choice >= 1 && choice <= backupFiles.Length)
                {
                    string selectedFile = backupFiles[choice - 1].FullName;
                    Console.Write($"Yakin ingin restore dari {backupFiles[choice - 1].Name}? (Y/N): ");
                    var confirm = Console.ReadLine()?.Trim().ToUpper();

                    if (confirm == "Y")
                    {
                        var backupLines = File.ReadAllLines(selectedFile)
                            .Where(line => line.StartsWith("INSERT INTO"))
                            .ToArray();

                        using var conn = new MySqlConnection(MySqlConnectionString);
                        conn.Open();

                        // Clear existing data
                        using var cmdClear = conn.CreateCommand();
                        cmdClear.CommandText = $"TRUNCATE TABLE `{MySqlTableName}`";
                        cmdClear.ExecuteNonQuery();

                        // Restore data
                        int successCount = 0;
                        foreach (var line in backupLines)
                        {
                            try
                            {
                                using var cmdInsert = conn.CreateCommand();
                                cmdInsert.CommandText = line;
                                cmdInsert.ExecuteNonQuery();
                                successCount++;
                            }
                            catch
                            {
                                // Continue with next record
                            }
                        }

                        ShowSuccess($"Restore BERHASIL! {successCount} records dipulihkan.");
                    }
                }
                else
                {
                    ShowError("Pilihan tidak valid.");
                }
            }
            catch (Exception ex)
            {
                ShowError($"Restore GAGAL: {ex.Message}");
            }
        }

        static void ExportMySqlToExcel(string filePath, string sheetName)
        {
            try
            {
                if (!MySqlEnabled || string.IsNullOrEmpty(MySqlConnectionString) || string.IsNullOrEmpty(MySqlTableName))
                {
                    ShowError("MySQL belum dikonfigurasi.");
                    return;
                }

                using var conn = new MySqlConnection(MySqlConnectionString);
                conn.Open();

                using var cmd = conn.CreateCommand();
                cmd.CommandText = $"SELECT * FROM `{MySqlTableName}` ORDER BY id";

                using var reader = cmd.ExecuteReader();

                // Clear existing Excel data (keep headers)
                var existingRows = ReadRows(filePath, sheetName);
                if (existingRows.Count > 1) // Keep header row
                {
                    for (int i = existingRows.Count - 1; i >= 1; i--)
                    {
                        DeleteRow(filePath, sheetName, i + 1);
                    }
                }

                int importedCount = 0;
                while (reader.Read())
                {
                    var record = new EntryRecord(
                        reader["Date"] == DBNull.Value ? "" : Convert.ToDateTime(reader["Date"]).ToString("yyyy-MM-dd"),
                        reader["Shift"]?.ToString() ?? "",
                        reader["CodeReference"]?.ToString() ?? "",
                        reader["MachineNumber"]?.ToString() ?? "",
                        reader["Area"]?.ToString() ?? "",
                        reader["AutoAdjustment"]?.ToString() ?? "",
                        reader["TopTec"]?.ToString() ?? "",
                        reader["FinalTester"]?.ToString() ?? "",
                        reader["Packaging"]?.ToString() ?? "",
                        reader["QuantityInput"] == DBNull.Value ? 0 : Convert.ToInt32(reader["QuantityInput"]),
                        reader["QuantityGood"] == DBNull.Value ? 0 : Convert.ToInt32(reader["QuantityGood"]),
                        reader["QuantityBad"] == DBNull.Value ? 0 : Convert.ToInt32(reader["QuantityBad"]),
                        reader["Reject"] == DBNull.Value ? 0 : Convert.ToInt32(reader["Reject"])
                    );

                    var values = record.ToValues();
                    var withId = new string[values.Length + 1];
                    withId[0] = reader["id"]?.ToString() ?? "";
                    Array.Copy(values, 0, withId, 1, values.Length);
                    AddRow(filePath, sheetName, withId);
                    importedCount++;
                }

                ShowSuccess($"Export MySQL to Excel BERHASIL! {importedCount} records diimpor.");
            }
            catch (Exception ex)
            {
                ShowError($"Export GAGAL: {ex.Message}");
            }
        }
        #endregion

        #region DATA MODELS
        class AppConfig
        {
            public string? DefaultFilePath { get; set; } = "Batam_Schneider_Entry_Data_File1.xlsx";
            public string? DefaultSheetName { get; set; } = "Data1";
            public bool AutoBackup { get; set; } = true;
            public int BackupRetentionDays { get; set; } = 30;
            public string? LastMySqlConnection { get; set; }

            public void Load()
            {
                try
                {
                    if (File.Exists("config.json"))
                    {
                        var json = File.ReadAllText("config.json");
                        var loaded = JsonSerializer.Deserialize<AppConfig>(json);
                        if (loaded != null)
                        {
                            DefaultFilePath = loaded.DefaultFilePath;
                            DefaultSheetName = loaded.DefaultSheetName;
                            AutoBackup = loaded.AutoBackup;
                            BackupRetentionDays = loaded.BackupRetentionDays;
                            LastMySqlConnection = loaded.LastMySqlConnection;
                        }
                    }
                }
                catch (Exception ex)
                {
                    LogToFile($"Error loading config: {ex.Message}");
                    // Use default values
                }
            }

            public void Save()
            {
                try
                {
                    var json = JsonSerializer.Serialize(this, new JsonSerializerOptions { WriteIndented = true });
                    File.WriteAllText("config.json", json);
                }
                catch (Exception ex)
                {
                    LogToFile($"Error saving config: {ex.Message}");
                }
            }
        }

        class OperationLog
        {
            public DateTime Timestamp { get; set; }
            public string? OperationType { get; set; }
            public string? Description { get; set; }
        }

        record EntryRecord(
            string Date,
            string Shift,
            string CodeReference,
            string MachineNumber,
            string Area,
            string ProcessAutoAdjustment,
            string ProcessTopTec,
            string ProcessFinalTester,
            string ProcessPackaging,
            int QuantityInput,
            int QuantityGood,
            int QuantityBad,
            int Reject
        )
        {
            public string[] ToValues() =>
                new string[]
                {
                   Date, Shift, CodeReference, MachineNumber, Area,
                   ProcessAutoAdjustment, ProcessTopTec, ProcessFinalTester, ProcessPackaging,
                   QuantityInput.ToString(), QuantityGood.ToString(), QuantityBad.ToString(), Reject.ToString()
                };

            public static string[] Headers => new[]
            {
               "Date","Shift","CodeReference","MachineNumber","Area",
               "AutoAdjustment","TopTec","FinalTester","Packaging",
               "QuantityInput","QuantityGood","QuantityBad","Reject"
           };
        }
        #endregion
    }
}