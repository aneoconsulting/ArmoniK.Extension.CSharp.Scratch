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

using System;

using ArmoniK.Extension.CSharp.Client.Exceptions;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;

namespace ArmoniK.Extension.CSharp.Client.Common;

/// <summary>
///   The properties class to set all configuration of the service
///   1. The connection information
///   2. The Option configuration AppSettings
///   The ssl mTLS certificate if needed to connect to the control plane
/// </summary>
public record Properties
{
  /// <summary>
  ///   Returns the section key Grpc from appSettings.json
  /// </summary>
  private const string Grpc = "Grpc";

  private const string EndPoint                  = "EndPoint";
  private const string SSlValidation             = "SSLValidation";
  private const string CaCert                    = "CaCert";
  private const string ClientCert                = "ClientCert";
  private const string ClientKey                 = "ClientKey";
  private const string ClientCertP12             = "ClientP12";
  private const string SectionTargetNameOverride = "EndpointNameOverride";

  private const string SectionRetryInitialBackoff    = "RetryInitialBackoff";
  private const string SectionRetryBackoffMultiplier = "RetryBackoffMultiplier";
  private const string SectionRetryMaxBackoff        = "RetryMaxBackoff";

  /// <summary>
  ///   The constructor to instantiate Properties object
  /// </summary>
  /// <param name="connectionAddress">The control plane address to connect</param>
  /// <param name="connectionPort">The optional port to connect to the control plane</param>
  /// <param name="protocol">the protocol https or http</param>
  /// <param name="clientCertPem">The client certificate fil in a pem format</param>
  /// <param name="clientKeyPem">The client key file in a pem format</param>
  /// <param name="clientP12">The client certificate in a P12/Pkcs12/PFX format</param>
  /// <param name="caCertPem">The Server certificate file to validate mTLS</param>
  /// <param name="sslValidation">Disable the ssl strong validation of ssl certificate (default : enable => true)</param>
  public Properties(string  connectionAddress,
                    int     connectionPort = 0,
                    string? protocol       = null,
                    string? clientCertPem  = null,
                    string? clientKeyPem   = null,
                    string? clientP12      = null,
                    string? caCertPem      = null,
                    bool?   sslValidation  = null)
    : this(new ConfigurationBuilder().AddEnvironmentVariables()
                                     .Build(),
           connectionAddress,
           connectionPort,
           protocol,
           clientCertPem,
           clientKeyPem,
           clientP12,
           caCertPem,
           sslValidation)
  {
  }

  /// <summary>
  ///   The constructor to instantiate Properties object
  /// </summary>
  /// <param name="configuration">The configuration to read information from AppSettings file</param>
  /// <param name="connectionAddress">The control plane address to connect</param>
  /// <param name="connectionPort">The optional port to connect to the control plane</param>
  /// <param name="protocol">the protocol https or http</param>
  /// <param name="caCertPem">The Server certificate file to validate mTLS</param>
  /// <param name="clientCertFilePem">The client certificate fil in a pem format</param>
  /// <param name="clientKeyFilePem">The client key file in a pem format</param>
  /// <param name="clientP12">The client certificate in a P12/Pkcs12/PFX format</param>
  /// <param name="sslValidation">Disable the ssl strong validation of ssl certificate (default : enable => true)</param>
  /// <param name="retryInitialBackoff">Initial retry backoff delay</param>
  /// <param name="retryBackoffMultiplier">Retry backoff multiplier</param>
  /// <param name="retryMaxBackoff">Max retry backoff</param>
  /// <exception cref="ArgumentException"></exception>
  public Properties(IConfiguration configuration,
                    string?        connectionAddress      = null,
                    int            connectionPort         = 0,
                    string?        protocol               = null,
                    string?        clientCertFilePem      = null,
                    string?        clientKeyFilePem       = null,
                    string?        clientP12              = null,
                    string?        caCertPem              = null,
                    bool?          sslValidation          = null,
                    TimeSpan       retryInitialBackoff    = new(),
                    double         retryBackoffMultiplier = 0,
                    TimeSpan       retryMaxBackoff        = new())
  {
    Configuration = configuration;

    var sectionGrpc = configuration.GetSection(Grpc);
    if (sectionGrpc == null)
    {
      throw new ArmoniKSdkException($"Missing section {Grpc} in configuration file");
    }

    if (connectionAddress != null)
    {
      var uri = new Uri(connectionAddress);
      ConnectionAddress = uri.Host;
      if (!string.IsNullOrEmpty(uri.Scheme))
      {
        Protocol = uri.Scheme;
      }
    }
    else
    {
      ConnectionAddress = string.Empty; // to remove a compiler message for netstandard2.0
      try
      {
        var connectionString = sectionGrpc.GetSection(EndPoint)
                                          .Value;
        if (!string.IsNullOrEmpty(connectionString))
        {
          var uri = new Uri(connectionString);
          Protocol          = uri.Scheme;
          ConnectionAddress = uri.Host;
          ConnectionPort    = uri.Port;
        }
      }
      catch (FormatException e)
      {
        Console.WriteLine(e);
        ConnectionAddress = string.Empty;
        ConnectionPort    = 0;
      }
    }

    Protocol = protocol ?? Protocol;

    ConfSslValidation  = sslValidation                          ?? sectionGrpc[SSlValidation] != "disable";
    TargetNameOverride = sectionGrpc[SectionTargetNameOverride] ?? string.Empty;
    CaCertFilePem      = caCertPem                              ?? sectionGrpc[CaCert]        ?? string.Empty;
    ClientCertFilePem  = clientCertFilePem                      ?? sectionGrpc[ClientCert]    ?? string.Empty;
    ClientKeyFilePem   = clientKeyFilePem                       ?? sectionGrpc[ClientKey]     ?? string.Empty;
    ClientP12File      = clientP12                              ?? sectionGrpc[ClientCertP12] ?? string.Empty;

    if (retryInitialBackoff != TimeSpan.Zero)
    {
      RetryInitialBackoff = retryInitialBackoff;
    }
    else if (!string.IsNullOrWhiteSpace(sectionGrpc[SectionRetryInitialBackoff]))
    {
      RetryInitialBackoff = TimeSpan.Parse(sectionGrpc[SectionRetryInitialBackoff]!);
    }

    if (retryBackoffMultiplier != 0)
    {
      RetryBackoffMultiplier = retryBackoffMultiplier;
    }
    else if (!string.IsNullOrWhiteSpace(sectionGrpc[SectionRetryBackoffMultiplier]))
    {
      RetryBackoffMultiplier = double.Parse(sectionGrpc[SectionRetryBackoffMultiplier]!);
    }

    if (retryMaxBackoff != TimeSpan.Zero)
    {
      RetryMaxBackoff = retryMaxBackoff;
    }
    else if (!string.IsNullOrWhiteSpace(sectionGrpc[SectionRetryMaxBackoff]))
    {
      RetryMaxBackoff = TimeSpan.Parse(sectionGrpc[SectionRetryMaxBackoff]!);
    }

    if (connectionPort != 0)
    {
      ConnectionPort = connectionPort;
    }

    if (string.IsNullOrEmpty(Protocol) || string.IsNullOrEmpty(ConnectionAddress) || ConnectionPort == 0)
    {
      throw new ArgumentException($"Issue with the connection point : {ConnectionString}");
    }

    ControlPlaneUri = new Uri(ConnectionString);
  }

  /// <summary>
  ///   The control plane url to connect
  /// </summary>
  public Uri ControlPlaneUri { get; init; }

  /// <summary>
  ///   The path to the CA Root file name
  /// </summary>
  public string CaCertFilePem { get; }

  /// <summary>
  ///   The property to get the path of the certificate file
  /// </summary>
  public string ClientCertFilePem { get; init; }

  /// <summary>
  ///   the property to get the path of the key certificate
  /// </summary>
  public string ClientKeyFilePem { get; init; }

  /// <summary>
  ///   the property to get the path of the certificate in P12/Pkcs12/PFX format
  /// </summary>
  public string ClientP12File { get; init; }

  /// <summary>
  ///   The SSL validation property to disable SSL strong verification
  /// </summary>
  [PublicAPI]
  [Obsolete("Use ConfSslValidation instead")]
  public bool ConfSSLValidation
    => ConfSslValidation;

  /// <summary>
  ///   The SSL validation property to disable SSL strong verification
  /// </summary>
  public bool ConfSslValidation { get; init; }

  /// <summary>
  ///   The configuration property to give to the ClientService connector
  /// </summary>
  public IConfiguration Configuration { get; init; }

  /// <summary>
  ///   The connection string building the value Port Protocol and address
  /// </summary>
  public string ConnectionString
    => $"{Protocol}://{ConnectionAddress}:{ConnectionPort}";

  /// <summary>
  ///   Secure or insecure protocol communication https or http (Default http)
  /// </summary>
  public string Protocol { get; init; } = "http";

  /// <summary>
  ///   The connection address property to connect to the control plane
  /// </summary>
  public string ConnectionAddress { get; init; }

  /// <summary>
  ///   The option connection port to connect to control plane (Default : 5001)
  /// </summary>
  public int ConnectionPort { get; init; } = 5001;

  /// <summary>
  ///   The target name of the endpoint when ssl validation is disabled. Automatic if not set.
  /// </summary>
  public string TargetNameOverride { get; init; } = "";

  /// <summary>
  ///   Initial backoff from retries
  /// </summary>
  public TimeSpan RetryInitialBackoff { get; init; } = TimeSpan.FromSeconds(1);

  /// <summary>
  ///   Backoff multiplier for retries
  /// </summary>
  public double RetryBackoffMultiplier { get; init; } = 2;

  /// <summary>
  ///   Max backoff for retries
  /// </summary>
  public TimeSpan RetryMaxBackoff { get; init; } = TimeSpan.FromSeconds(30);
}
