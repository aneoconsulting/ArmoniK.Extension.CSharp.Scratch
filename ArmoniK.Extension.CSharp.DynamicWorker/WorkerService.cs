// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.IO.Compression;
using System.Reflection;
using System.Runtime.Loader;

using ArmoniK.Api.Worker.Worker;
using ArmoniK.Extension.CSharp.Common.Exceptions;
using ArmoniK.Extension.CSharp.Common.Library;
using ArmoniK.Extension.CSharp.Worker.Interfaces;

namespace ArmoniK.Extension.CSharp.DynamicWorker;

internal sealed class WorkerService : IDisposable
{
  private readonly string  extractionPath_;
  private readonly object  locker_ = new();
  private readonly ILogger logger_;
  private          bool    disposed_;

  private WorkerService(string              serviceName,
                        string              extractionPath,
                        AssemblyLoadContext loadContext,
                        IWorker             worker,
                        ILogger             logger)
  {
    ServiceName           =  serviceName;
    extractionPath_       =  extractionPath;
    LoadContext           =  loadContext;
    Worker                =  worker;
    logger_               =  logger;
    LoadContext.Unloading += OnUnload;
  }

  public string               ServiceName { get; init; }
  public IWorker              Worker      { get; private set; }
  public AssemblyLoadContext? LoadContext { get; set; }

  public void Dispose()
  {
    if (!disposed_)
    {
      Worker = null!;
      LoadContext!.Unload();
      LoadContext = null;
      disposed_   = true;
    }
  }

  private void OnUnload(AssemblyLoadContext context)
  {
    logger_.LogInformation("Service {Service} Unloaded",
                           ServiceName);
    context.Unloading -= OnUnload;
  }

  public static async Task<WorkerService> CreateWorkerService(ITaskHandler      taskHandler,
                                                              DynamicLibrary    dynamicLibrary,
                                                              ILoggerFactory    loggerFactory,
                                                              CancellationToken cancellationToken)
  {
    var zipFilename = $"{dynamicLibrary}.zip";
    var zipPath     = @"/tmp/zip";
    var unzipPath   = @"/tmp/assemblies";
    var libraryPath = dynamicLibrary.LibraryPath;
    var loadContext = new AssemblyLoadContext(dynamicLibrary.Symbol,
                                              true);
    var logger = loggerFactory.CreateLogger<WorkerService>();

    logger.LogInformation($"Starting Dynamic loading - FileName: {zipFilename}, FilePath: {zipPath}, DestinationToUnZip:{unzipPath}, LibraryPath:{libraryPath}, Symbol: {dynamicLibrary.Symbol}");

    var dllExists = taskHandler.DataDependencies.TryGetValue(dynamicLibrary.LibraryBlobId,
                                                             out var libraryBytes);
    if (!dllExists || libraryBytes is null)
    {
      throw new ArmoniKSdkException($"No library found on data dependencies. (Library BlobId is {dynamicLibrary.LibraryBlobId})");
    }

    try
    {
      Directory.CreateDirectory(zipPath);

      // Create the full path to the zip file
      var zipFilePath = Path.Combine(zipPath,
                                     zipFilename);

      await File.WriteAllBytesAsync(zipFilePath,
                                    libraryBytes,
                                    cancellationToken)
                .ConfigureAwait(false);

      logger.LogInformation("Extracting from archive {localZip}",
                            zipFilePath);

      var libUnzipPath = Path.Combine(unzipPath,
                                      dynamicLibrary.LibraryBlobId);
      var extractedFilePath = ExtractArchive(zipFilename,
                                             zipPath,
                                             libUnzipPath,
                                             libraryPath,
                                             logger);
      File.Delete(zipFilePath);

      logger.LogInformation("Package {dynamicLibrary} successfully extracted from {localAssembly}",
                            dynamicLibrary,
                            extractedFilePath);

      logger.LogInformation("Trying to load: {dllPath}",
                            extractedFilePath);

      var assembly = loadContext.LoadFromAssemblyPath(extractedFilePath);
      var workerInstance = GetClassInstance<IWorker>(dynamicLibrary,
                                                     loadContext,
                                                     logger,
                                                     assembly);
      return new WorkerService(dynamicLibrary.Symbol,
                               libUnzipPath,
                               loadContext,
                               workerInstance,
                               logger);
    }
    catch (Exception ex)
    {
      throw new ArmoniKSdkException(ex);
    }
  }

  /// <summary>
  ///   Extracts the archive to the specified destination.
  /// </summary>
  /// <param name="zipFilename">The name of the ZIP file.</param>
  /// <param name="zipPath">The path to the ZIP file.</param>
  /// <param name="unzipPath">The destination path to extract the files to.</param>
  /// <param name="libraryPath">The path to the DLL file within the extracted files.</param>
  /// <param name="logger">The logger.</param>
  /// <returns>The path to the DLL file within the destination directory.</returns>
  /// <exception cref="ArmoniKSdkException">Thrown when the extraction fails or the file is not a ZIP file.</exception>
  private static string ExtractArchive(string  zipFilename,
                                       string  zipPath,
                                       string  unzipPath,
                                       string  libraryPath,
                                       ILogger logger)
  {
    if (!IsZipFile(zipFilename))
    {
      throw new ArmoniKSdkException("Cannot yet extract or manage raw data other than zip archive");
    }

    var dllFile = Path.Join(unzipPath,
                            libraryPath);

    if (!File.Exists(dllFile))
    {
      var temporaryDirectory = "";
      try
      {
        var originFile = Path.Join(zipPath,
                                   zipFilename);

        if (!Directory.Exists(unzipPath))
        {
          Directory.CreateDirectory(unzipPath);
        }

        temporaryDirectory = Path.Combine(Path.GetTempPath(),
                                          Guid.NewGuid()
                                              .ToString());
        Directory.CreateDirectory(temporaryDirectory);

        logger.LogInformation("Dll should be in the following folder {dllFile}",
                              dllFile);

        ZipFile.ExtractToDirectory(originFile,
                                   temporaryDirectory);

        logger.LogInformation("Extracted zip file");

        logger.LogInformation("Moving unzipped file");
        MoveDirectoryContent(temporaryDirectory,
                             unzipPath,
                             logger);
      }
      catch (Exception e)
      {
        throw new ArmoniKSdkException(e);
      }
      finally
      {
        if (File.Exists(temporaryDirectory))
        {
          File.Delete(temporaryDirectory);
        }
      }
    }
    else
    {
      logger.LogInformation("Could not extract zip, file exists already");
    }

    if (!File.Exists(dllFile))
    {
      logger.LogError("Dll should be in the following folder {dllFile}",
                      dllFile);
      throw new ArmoniKSdkException($"Fail to find assembly {dllFile}");
    }

    return dllFile;
  }

  /// <summary>
  ///   Moves the content of the source directory to the destination directory.
  /// </summary>
  /// <param name="sourceDirectory">The source directory.</param>
  /// <param name="destinationDirectory">The destination directory.</param>
  /// <param name="logger">The logger.</param>
  /// <exception cref="ArmoniKSdkException">Thrown when there is an error moving the directory content.</exception>
  private static void MoveDirectoryContent(string  sourceDirectory,
                                           string  destinationDirectory,
                                           ILogger logger)
  {
    try
    {
      // Create all directories in destination if they do not exist
      foreach (var dirPath in Directory.GetDirectories(sourceDirectory,
                                                       "*",
                                                       SearchOption.AllDirectories))
      {
        Directory.CreateDirectory(dirPath.Replace(sourceDirectory,
                                                  destinationDirectory));
      }

      // Move all files from the source to the destination
      foreach (var newPath in Directory.GetFiles(sourceDirectory,
                                                 "*.*",
                                                 SearchOption.AllDirectories))
      {
        File.Move(newPath,
                  newPath.Replace(sourceDirectory,
                                  destinationDirectory));
      }

      // Optionally, delete the source directory if needed
      Directory.Delete(sourceDirectory,
                       true);

      logger.LogInformation("All files and folders have been moved successfully.");
    }
    catch (Exception ex)
    {
      logger.LogError(ex,
                      "Could not move file");
      throw;
    }
  }

  /// <summary>
  ///   Determines whether the specified file is a ZIP file.
  /// </summary>
  /// <param name="assemblyNameFilePath">The file path of the assembly.</param>
  /// <returns><c>true</c> if the file is a ZIP file; otherwise, <c>false</c>.</returns>
  public static bool IsZipFile(string assemblyNameFilePath)
  {
    var extension = Path.GetExtension(assemblyNameFilePath);
    return extension?.ToLower() == ".zip";
  }

  /// <summary>
  ///   Gets an instance of a class from the dynamic library.
  /// </summary>
  /// <typeparam name="T">Type that the created instance must be convertible to.</typeparam>
  /// <param name="dynamicLibrary">The dynamic library definition.</param>
  /// <param name="loadContext">The load context.</param>
  /// <param name="logger">The logger.</param>
  /// <param name="assembly">The library's assembly.</param>
  /// <returns>An instance of the class specified by <paramref name="dynamicLibrary" />.</returns>
  /// <exception cref="ArmoniKSdkException">Thrown when there is an error loading the class instance.</exception>
  private static T GetClassInstance<T>(DynamicLibrary      dynamicLibrary,
                                       AssemblyLoadContext loadContext,
                                       ILogger             logger,
                                       Assembly            assembly)
    where T : class
  {
    try
    {
      using (loadContext.EnterContextualReflection())
      {
        // Create an instance of a class from the assembly.
        var classType = assembly.GetType($"{dynamicLibrary.Symbol}");
        logger.LogInformation("Types found in the assembly: {assemblyTypes}",
                              string.Join(",",
                                          assembly.GetTypes()
                                                  .Select(x => x.ToString())));
        if (classType is null)
        {
          var message = $"Failure to create an instance of {dynamicLibrary.Symbol}: type not found";
          logger.LogError(message);
          throw new ArmoniKSdkException(message);
        }

        logger.LogInformation($"Type {dynamicLibrary.Symbol}: {classType} loaded");

        var serviceContainer = Activator.CreateInstance(classType);
        if (serviceContainer is null)
        {
          var message = $"Could not create an instance of type {classType.Name} (default constructor missing?)";
          logger.LogError(message);
          throw new ArmoniKSdkException(message);
        }

        var typedServiceContainer = serviceContainer as T;
        if (typedServiceContainer is null)
        {
          var message = $"The type {classType.Name} is not convertible to {typeof(T)}";
          logger.LogError(message);
          throw new ArmoniKSdkException(message);
        }

        return typedServiceContainer;
      }
    }
    catch (Exception e)
    {
      logger.LogError("Error loading class instance: {errorMessage}",
                      e.Message);
      throw new ArmoniKSdkException(e);
    }
  }
}
