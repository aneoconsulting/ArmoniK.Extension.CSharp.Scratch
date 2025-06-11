// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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

using System.Collections.Concurrent;
using System.IO.Compression;
using System.Reflection;
using System.Runtime.Loader;

using ArmoniK.Api.Worker.Worker;
using ArmoniK.Extension.CSharp.DllCommon;
using ArmoniK.Extension.CSharp.Worker;

namespace ArmoniK.Extension.CSharp.DllWorker;

/// <summary>
///   Provides functionality to load and manage dynamic libraries for the ArmoniK project.
/// </summary>
public class LibraryLoader : ILibraryLoader
{
  private readonly ILogger                                                                            logger_;
  private          ConcurrentDictionary<string, (Assembly assembly, AssemblyLoadContext loadContext)> assemblyLoadContexts_ = new();

  /// <summary>
  ///   Initializes a new instance of the <see cref="LibraryLoader" /> class.
  /// </summary>
  /// <param name="loggerFactory">The logger factory to create logger instances.</param>
  public LibraryLoader(ILoggerFactory loggerFactory)
    => logger_ = loggerFactory.CreateLogger<LibraryLoader>();

  /// <summary>
  ///   Disposes the current instance and resets the assembly load contexts.
  /// </summary>
  public void Dispose()
    => assemblyLoadContexts_ = new ConcurrentDictionary<string, (Assembly, AssemblyLoadContext)>();

  /// <summary>
  ///   Gets the assembly load context for the specified library context key.
  /// </summary>
  /// <param name="libraryContextKey">The key of the library context.</param>
  /// <returns>The assembly load context associated with the specified key.</returns>
  /// <exception cref="WorkerApiException">Thrown when the key is not found in the dictionary.</exception>
  public AssemblyLoadContext GetAssemblyLoadContext(string libraryContextKey)
  {
    var exists = assemblyLoadContexts_.TryGetValue(libraryContextKey,
                                                   out var value);
    if (!exists)
    {
      logger_.LogError($"AssemblyLoadContexts does not have key {libraryContextKey}");
      throw new WorkerApiException("No key found on AssemblyLoadContexts dictionary");
    }

    return value.loadContext;
  }

  /// <summary>
  ///   Resets the service by disposing the current instance.
  /// </summary>
  public void ResetService()
    => Dispose();

  /// <summary>
  ///   Gets an instance of a class from the dynamic library.
  /// </summary>
  /// <typeparam name="T">Type that the created instance must be convertible to.</typeparam>
  /// <param name="dynamicLibrary">The dynamic library definition.</param>
  /// <returns>An instance of the class specified by <paramref name="dynamicLibrary" />.</returns>
  /// <exception cref="WorkerApiException">Thrown when there is an error loading the class instance.</exception>
  public T GetClassInstance<T>(TaskLibraryDefinition dynamicLibrary)
    where T : class
  {
    try
    {
      assemblyLoadContexts_.TryGetValue(dynamicLibrary.ToString(),
                                        out var assembly);
      using (assembly.loadContext.EnterContextualReflection())
      {
        // Create an instance of a class from the assembly.
        var classType = assembly.assembly.GetType($"{dynamicLibrary.Namespace}.{dynamicLibrary.Service}");
        logger_.LogInformation("Types found in the assembly: {assemblyTypes}",
                               string.Join(",",
                                           assembly.assembly.GetTypes()
                                                   .Select(x => x.ToString())));
        if (classType is null)
        {
          var message = $"Type {dynamicLibrary.Namespace}.{dynamicLibrary.Service} could not be loaded";
          logger_.LogError(message);
          throw new WorkerApiException(message);
        }

        logger_.LogInformation($"Type {dynamicLibrary.Namespace}.{dynamicLibrary.Service}: {classType} loaded");

        var serviceContainer = Activator.CreateInstance(classType);
        if (serviceContainer is null)
        {
          var message = $"Could not create an instance of type {classType.Name} (default constructor missing?)";
          logger_.LogError(message);
          throw new WorkerApiException(message);
        }

        var typedServiceContainer = serviceContainer as T;
        if (typedServiceContainer is null)
        {
          var message = $"The type {classType.Name} is not convertible to {typeof(T)}";
          logger_.LogError(message);
          throw new WorkerApiException(message);
        }

        return typedServiceContainer;
      }
    }
    catch (Exception e)
    {
      logger_.LogError("Error loading class instance: {errorMessage}",
                       e.Message);
      throw new WorkerApiException(e);
    }
  }

  /// <summary>
  ///   Loads a library asynchronously based on the task handler and cancellation token provided.
  /// </summary>
  /// <param name="taskHandler">The task handler containing the task options.</param>
  /// <param name="cancellationToken">The cancellation token to cancel the operation.</param>
  /// <returns>A task representing the asynchronous operation, containing the name of the dynamic library loaded.</returns>
  /// <exception cref="WorkerApiException">Thrown when there is an error loading the library.</exception>
  public async Task<string> LoadLibraryAsync(ITaskHandler      taskHandler,
                                             CancellationToken cancellationToken)
  {
    try
    {
      logger_.LogInformation("Starting to LoadLibrary");
      logger_.LogInformation("Nb of current loaded assemblies: {nbAssemblyLoadContexts}",
                             assemblyLoadContexts_.Count);

      var taskLibrary = taskHandler.TaskOptions.GetServiceLibrary();
      if (string.IsNullOrEmpty(taskLibrary))
      {
        throw new KeyNotFoundException("Task option 'ServiceLibrary' not found");
      }

      // Get the data about the dynamic library
      var dynamicLibrary = taskHandler.TaskOptions.GetTaskLibraryDefinition(taskLibrary);

      var filename = $"{dynamicLibrary}.zip";

      var filePath = @"/tmp/zip";

      var destinationPath = @"/tmp/assemblies";

      var pathToDllFile = dynamicLibrary.PathToFile;

      var dllFileName = dynamicLibrary.DllFileName;

      logger_.LogInformation("Starting Dynamic loading - TaskLibrary: {taskLibrary}, FileName: {filename}, FilePath: {filePath}, DestinationToUnZip:{destinationPath}, PathToDllFile:{pathToDllFile}, DllFileName: {dllFileName}, Namespace: {dynamicLibrary.Namespace}, Service: {dynamicLibrary.Service}",
                             taskLibrary,
                             filename,
                             filePath,
                             destinationPath,
                             pathToDllFile,
                             dllFileName,
                             dynamicLibrary.Namespace,
                             dynamicLibrary.Service);

      // if the context is already loaded
      if (assemblyLoadContexts_.ContainsKey(dynamicLibrary.ToString()))
      {
        return dynamicLibrary.ToString();
      }

      var loadContext = new AssemblyLoadContext(dynamicLibrary.ToString());

      var dllExists = taskHandler.DataDependencies.TryGetValue(dynamicLibrary.LibraryBlobId,
                                                               out var libraryBytes);
      if (!dllExists || libraryBytes is null)
      {
        throw new WorkerApiException("No library found on data dependencies.");
      }

      try
      {
        Directory.CreateDirectory(filePath);

        // Create the full path to the zip file
        var zipFilePath = Path.Combine(filePath,
                                       filename);

        await File.WriteAllBytesAsync(zipFilePath,
                                      libraryBytes,
                                      cancellationToken)
                  .ConfigureAwait(false);
      }
      catch (Exception ex)
      {
        throw new WorkerApiException(ex);
      }

      logger_.LogInformation("Extracting from archive {localZip}",
                             Path.Join(filePath,
                                       filename));

      var extractedPath = ExtractArchive(filename,
                                         filePath,
                                         destinationPath,
                                         pathToDllFile,
                                         dllFileName);

      var zipFile = Path.Join(filePath,
                              filename);

      File.Delete(zipFile);

      logger_.LogInformation("Package {dynamicLibrary} successfully extracted from {localAssembly}",
                             dynamicLibrary,
                             extractedPath);

      logger_.LogInformation("Trying to load: {dllPath}",
                             Path.Join(extractedPath,
                                       dllFileName));

      var assembly = loadContext.LoadFromAssemblyPath(Path.Join(extractedPath,
                                                                dllFileName));

      if (!assemblyLoadContexts_.TryAdd(dynamicLibrary.ToString(),
                                        (assembly, loadContext)))
      {
        throw new WorkerApiException($"Unable to add load context {dynamicLibrary}");
      }

      logger_.LogInformation("Nb of current loaded assemblies: {nbAssemblyLoadContexts}",
                             assemblyLoadContexts_.Count);

      return dynamicLibrary.ToString();
    }
    catch (Exception ex)
    {
      logger_.LogError(ex.Message);
      throw new WorkerApiException(ex);
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
  ///   Extracts the archive to the specified destination.
  /// </summary>
  /// <param name="filename">The name of the ZIP file.</param>
  /// <param name="filePath">The path to the ZIP file.</param>
  /// <param name="destinationPath">The destination path to extract the files to.</param>
  /// <param name="pathToDllFile">The path to the DLL file within the extracted files.</param>
  /// <param name="dllFileName">The name of the DLL file.</param>
  /// <param name="overwrite">Whether to overwrite existing files.</param>
  /// <returns>The path to the DLL file within the destination directory.</returns>
  /// <exception cref="WorkerApiException">Thrown when the extraction fails or the file is not a ZIP file.</exception>
  public string ExtractArchive(string filename,
                               string filePath,
                               string destinationPath,
                               string pathToDllFile,
                               string dllFileName,
                               bool   overwrite = false)
  {
    if (!IsZipFile(filename))
    {
      throw new WorkerApiException("Cannot yet extract or manage raw data other than zip archive");
    }

    var originFile = Path.Join(filePath,
                               filename);

    if (!Directory.Exists(destinationPath))
    {
      Directory.CreateDirectory(destinationPath);
    }

    var dllFile = Path.Join(destinationPath,
                            pathToDllFile,
                            dllFileName);

    var temporaryDirectory = Path.Join(destinationPath,
                                       $"zip-{Guid.NewGuid()}");

    Directory.CreateDirectory(temporaryDirectory);

    logger_.LogInformation("Dll should be in the following folder {dllFile}",
                           dllFile);

    if (overwrite || !File.Exists(dllFile))
    {
      try
      {
        ZipFile.ExtractToDirectory(originFile,
                                   temporaryDirectory);

        logger_.LogInformation("Extracted zip file");

        logger_.LogInformation("Moving unzipped file");
        MoveDirectoryContent(temporaryDirectory,
                             destinationPath);
      }
      catch (Exception e)
      {
        throw new WorkerApiException(e);
      }
    }
    else
    {
      logger_.LogInformation("Could not extract zip, file exists already");
    }

    if (!File.Exists(dllFile))
    {
      logger_.LogError("Dll should in the following folder {dllFile}",
                       dllFile);
      throw new WorkerApiException($"Fail to find assembly {dllFile}");
    }

    return Path.Join(destinationPath,
                     pathToDllFile);
  }

  /// <summary>
  ///   Moves the content of the source directory to the destination directory.
  /// </summary>
  /// <param name="sourceDirectory">The source directory.</param>
  /// <param name="destinationDirectory">The destination directory.</param>
  /// <exception cref="WorkerApiException">Thrown when there is an error moving the directory content.</exception>
  public void MoveDirectoryContent(string sourceDirectory,
                                   string destinationDirectory)
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

      logger_.LogInformation("All files and folders have been moved successfully.");
    }
    catch (Exception ex)
    {
      logger_.LogError(ex,
                       "Could not move file");
      throw;
    }
  }
}
