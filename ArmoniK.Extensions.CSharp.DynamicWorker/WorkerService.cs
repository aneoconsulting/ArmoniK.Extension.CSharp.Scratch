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
using ArmoniK.Extensions.CSharp.Common.Exceptions;
using ArmoniK.Extensions.CSharp.Common.Library;
using ArmoniK.Extensions.CSharp.Worker.Interfaces;

namespace ArmoniK.Extensions.CSharp.DynamicWorker;

internal sealed class WorkerService : IDisposable
{
  private readonly ILogger logger_;

  private WorkerService(string              serviceName,
                        AssemblyLoadContext loadContext,
                        IWorker             worker,
                        ILogger             logger)
  {
    ServiceName           =  serviceName;
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
    Worker = null!;
    LoadContext?.Unload();
    LoadContext = null;
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

    try
    {
      logger.LogInformation($"Starting Dynamic loading - FileName: {zipFilename}, FilePath: {zipPath}, DestinationToUnZip:{unzipPath}, LibraryPath:{libraryPath}, Symbol: {dynamicLibrary.Symbol}");
      var libraryBytes = taskHandler.DataDependencies[dynamicLibrary.LibraryBlobId];
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
                                             dynamicLibrary,
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
  /// <param name="library">The dynamic library.</param>
  /// <param name="logger">The logger.</param>
  /// <returns>The path to the DLL file within the destination directory.</returns>
  /// <exception cref="ArmoniKSdkException">Thrown when the extraction fails or the file is not a ZIP file.</exception>
  private static string ExtractArchive(string         zipFilename,
                                       string         zipPath,
                                       string         unzipPath,
                                       DynamicLibrary library,
                                       ILogger        logger)
  {
    if (!IsZipFile(zipFilename))
    {
      logger.LogError("A zip archive was expected instead of {zipFilename}",
                      zipFilename);
      throw new ArmoniKSdkException($"A zip archive was expected instead of {zipFilename}");
    }

    var libraryPath   = library.LibraryPath;
    var librarySymbol = library.Symbol;
    var dllFile = Path.Join(unzipPath,
                            libraryPath);
    var signalPath = Path.Combine(unzipPath,
                                  $".{librarySymbol}.{library.LibraryBlobId}.extracted");

    if (File.Exists(signalPath))
    {
      return dllFile;
    }

    var zipFileFullpath = Path.Join(zipPath,
                                    zipFilename);
    var temporaryDirectory = Path.Combine(Path.GetTempPath(),
                                          Guid.NewGuid()
                                              .ToString());
    try
    {
      FileExt.TryCreateDirectory(temporaryDirectory);
    }
    catch (Exception ex)
    {
      logger.LogError("Cannot create temporary extraction directory {temporaryDirectory}",
                      temporaryDirectory);
      throw new ArmoniKSdkException($"Cannot create temporary extraction directory {temporaryDirectory}",
                                    ex);
    }

    try
    {
      logger.LogInformation("Extracting zip file to {dir} ...",
                            temporaryDirectory);
      ZipFile.ExtractToDirectory(zipFileFullpath,
                                 temporaryDirectory);
    }
    catch (Exception ex)
    {
      logger.LogError("Cannot extract archive {originFile} to directory {temporaryDirectory}",
                      zipFileFullpath,
                      temporaryDirectory);
      throw new ArmoniKSdkException($"Cannot extract archive {zipFileFullpath} to directory {temporaryDirectory}",
                                    ex);
    }

    try
    {
      logger.LogInformation("Moving unzipped file to {dir} ...",
                            unzipPath);
      FileExt.MoveDirectoryContent(temporaryDirectory,
                                   unzipPath);
      logger.LogInformation("All files and folders have been moved successfully.");
    }
    catch (Exception ex)
    {
      logger.LogError("Cannot move extracted archive {originFile} from directory {temporaryDirectory} to directory {unzipPath}",
                      zipFileFullpath,
                      temporaryDirectory,
                      unzipPath);
      throw new ArmoniKSdkException($"Cannot move extracted archive {zipFileFullpath} from directory {temporaryDirectory} to directory {unzipPath}",
                                    ex);
    }

    if (!File.Exists(dllFile))
    {
      logger.LogError("Fail to find assembly {dllFile} from extraction of {originFile}",
                      dllFile,
                      zipFileFullpath);
      throw new ArmoniKSdkException($"Fail to find assembly {dllFile} from extraction of {zipFileFullpath}");
    }

    FileExt.TryDeleteDirectory(temporaryDirectory);

    try
    {
      File.CreateText(signalPath)
          .Dispose();
    }
    catch (Exception ex)
    {
      if (!File.Exists(signalPath))
      {
        logger.LogError("Cannot finalize extraction of {dllFile}",
                        dllFile);
        throw new ArmoniKSdkException($"Cannot finalize extraction of {dllFile}",
                                      ex);
      }
    }

    return dllFile;
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
