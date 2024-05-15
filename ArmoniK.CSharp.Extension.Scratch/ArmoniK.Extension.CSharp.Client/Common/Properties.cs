﻿using ArmoniK.Api.gRPC.V1;
using Google.Protobuf.WellKnownTypes;
using JetBrains.Annotations;
using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Configuration;

namespace ArmoniK.Extension.CSharp.Client.Common
{
    /// <summary>
    ///   The properties class to set all configuration of the service
    ///   1. The connection information
    ///   2. The Option configuration AppSettings
    ///   The ssl mTLS certificate if needed to connect to the control plane
    /// </summary>
    [MarkDownDoc]
    // TODO: check all setter and mark the required as PublicApi
    // TODO: to be reworked to allow all options from API and add other elements.
    public class Properties
    {
        /// <summary>
        ///   Returns the section key Grpc from appSettings.json
        /// </summary>
        private const string SectionGrpc = "Grpc";

        private const string SectionEndPoint = "EndPoint";
        private const string SectionSSlValidation = "SSLValidation";
        private const string SectionCaCert = "CaCert";
        private const string SectionClientCert = "ClientCert";
        private const string SectionClientKey = "ClientKey";
        private const string SectionClientCertP12 = "ClientP12";
        private const string SectionTargetNameOverride = "EndpointNameOverride";

        private const string SectionRetryInitialBackoff = "RetryInitialBackoff";
        private const string SectionRetryBackoffMultiplier = "RetryBackoffMultiplier";
        private const string SectionRetryMaxBackoff = "RetryMaxBackoff";

        /// <summary>
        ///   The default configuration to submit task in a Session
        /// </summary>
        // TODO: define [PublicApi] ?
        // ReSharper disable once UnusedMember.Global
        public static readonly TaskOptions DefaultTaskOptions = new()
        {
            MaxDuration = new Duration
            {
                Seconds = 300000,
            },
            MaxRetries = 3,
            Priority = 1,
        };


        /// <summary>
        ///   The constructor to instantiate Properties object
        /// </summary>
        /// <param name="options">The taskOptions to set to a session</param>
        /// <param name="connectionAddress">The control plane address to connect</param>
        /// <param name="connectionPort">The optional port to connect to the control plane</param>
        /// <param name="protocol">the protocol https or http</param>
        /// <param name="clientCertPem">The client certificate fil in a pem format</param>
        /// <param name="clientKeyPem">The client key file in a pem format</param>
        /// <param name="clientP12">The client certificate in a P12/Pkcs12/PFX format</param>
        /// <param name="caCertPem">The Server certificate file to validate mTLS</param>
        /// <param name="sslValidation">Disable the ssl strong validation of ssl certificate (default : enable => true)</param>
        // TODO: define [PublicApi] ?
        // ReSharper disable once UnusedMember.Global
        public Properties(TaskOptions options,
            string connectionAddress,
            int connectionPort = 0,
            string protocol = null,
            string clientCertPem = null,
            string clientKeyPem = null,
            string clientP12 = null,
            string caCertPem = null,
            bool? sslValidation = null)
            : this(new ConfigurationBuilder().AddEnvironmentVariables()
                    .Build(),
                options,
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
        /// <param name="options">The taskOptions to set to a session</param>
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
            TaskOptions options,
            string connectionAddress = null,
            int connectionPort = 0,
            string protocol = null,
            string clientCertFilePem = null,
            string clientKeyFilePem = null,
            string clientP12 = null,
            string caCertPem = null,
            bool? sslValidation = null,
            TimeSpan retryInitialBackoff = new(),
            double retryBackoffMultiplier = 0,
            TimeSpan retryMaxBackoff = new())
        {
            TaskOptions = options;
            Configuration = configuration;

            var sectionGrpc = configuration.GetSection(SectionGrpc);

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
                    var connectionString = sectionGrpc.GetSection(SectionEndPoint).Value;
                    if (!string.IsNullOrEmpty(connectionString))
                    {
                        var uri = new Uri(connectionString);

                        Protocol = uri.Scheme;

                        ConnectionAddress = uri.Host;
                        ConnectionPort = uri.Port;
                    }
                }
                catch (FormatException e)
                {
                    Console.WriteLine(e);
                    ConnectionAddress = string.Empty;
                    ConnectionPort = 0;
                }
            }

            Protocol = protocol ?? Protocol;

            ConfSslValidation = sslValidation ?? sectionGrpc[SectionSSlValidation] != "disable";
            TargetNameOverride = sectionGrpc[SectionTargetNameOverride] ?? string.Empty;
            CaCertFilePem = caCertPem ?? sectionGrpc[SectionCaCert] ?? string.Empty;
            ClientCertFilePem = clientCertFilePem ?? sectionGrpc[SectionClientCert] ?? string.Empty;
            ClientKeyFilePem = clientKeyFilePem ?? sectionGrpc[SectionClientKey] ?? string.Empty;
            ClientP12File = clientP12 ?? sectionGrpc[SectionClientCertP12] ?? string.Empty;

            if (retryInitialBackoff != TimeSpan.Zero)
            {
                RetryInitialBackoff = retryInitialBackoff;
            }
            else if (!string.IsNullOrWhiteSpace(sectionGrpc?[SectionRetryInitialBackoff]))
            {
                RetryInitialBackoff = TimeSpan.Parse(sectionGrpc[SectionRetryInitialBackoff]);
            }

            if (retryBackoffMultiplier != 0)
            {
                RetryBackoffMultiplier = retryBackoffMultiplier;
            }
            else if (!string.IsNullOrWhiteSpace(sectionGrpc?[SectionRetryBackoffMultiplier]))
            {
                RetryBackoffMultiplier = double.Parse(sectionGrpc[SectionRetryBackoffMultiplier]);
            }


            if (retryMaxBackoff != TimeSpan.Zero)
            {
                RetryMaxBackoff = retryMaxBackoff;
            }
            else if (!string.IsNullOrWhiteSpace(sectionGrpc?[SectionRetryMaxBackoff]))
            {
                RetryMaxBackoff = TimeSpan.Parse(sectionGrpc[SectionRetryMaxBackoff]);
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
        ///   Set the number of task by buffer
        /// </summary>
        // TODO: mark as [PublicApi] for setter ?
        // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global
        public int MaxTasksPerBuffer { get; set; } = 500;


        /// <summary>
        ///   Set the number of buffers that can be filled in asynchronous submitAsync
        /// </summary>
        // TODO: mark as [PublicApi] for setter ?
        // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global
        public int MaxConcurrentBuffers { get; set; } = 1;


        /// <summary>
        ///   TimeSpan to trigger a batch to send the batch of submit
        /// </summary>
        // TODO: mark as [PublicApi] for setter ?
        // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global
        public TimeSpan? TimeTriggerBuffer { get; set; } = TimeSpan.FromSeconds(10);

        /// <summary>
        ///   The number of channels used for Buffered Submit (Default 1)
        /// </summary>
        // TODO: mark as [PublicApi] for setter ?
        // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global
        public int MaxParallelChannels { get; set; } = 1;

        /// <summary>
        ///   The control plane url to connect
        /// </summary>
        // TODO: mark as [PublicApi] for setter ?
        // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global
        public Uri ControlPlaneUri { get; set; }

        /// <summary>
        ///   The path to the CA Root file name
        /// </summary>
        // TODO: mark as [PublicApi] for setter ?
        // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global
        public string CaCertFilePem { get; set; }

        /// <summary>
        ///   The property to get the path of the certificate file
        /// </summary>
        public string ClientCertFilePem { get; }

        /// <summary>
        ///   the property to get the path of the key certificate
        /// </summary>
        public string ClientKeyFilePem { get; }

        /// <summary>
        ///   the property to get the path of the certificate in P12/Pkcs12/PFX format
        /// </summary>
        public string ClientP12File { get; }

        /// <summary>
        ///   The SSL validation property to disable SSL strong verification
        /// </summary>
        [PublicAPI]
        [Obsolete("Use ConfSslValidation instead")]
        // ReSharper disable once InconsistentNaming
        public bool ConfSSLValidation
            => ConfSslValidation;

        /// <summary>
        ///   The SSL validation property to disable SSL strong verification
        /// </summary>
        public bool ConfSslValidation { get; }

        /// <summary>
        ///   The configuration property to give to the ClientService connector
        /// </summary>
        // TODO: mark as [PublicApi] ?
        // ReSharper disable once UnusedAutoPropertyAccessor.Global
        public IConfiguration Configuration { get; }

        /// <summary>
        ///   The connection string building the value Port Protocol and address
        /// </summary>
        // TODO: mark as [PublicApi] ?
        // ReSharper disable once MemberCanBePrivate.Global
        public string ConnectionString
            => $"{Protocol}://{ConnectionAddress}:{ConnectionPort}";

        /// <summary>
        ///   Secure or insecure protocol communication https or http (Default http)
        /// </summary>
        // TODO: mark as [PublicApi] for setter ?
        // ReSharper disable once MemberCanBePrivate.Global
        public string Protocol { get; set; } = "http";

        /// <summary>
        ///   The connection address property to connect to the control plane
        /// </summary>
        // TODO: mark as [PublicApi] for setter ?
        // ReSharper disable once MemberCanBePrivate.Global
        public string ConnectionAddress { get; set; }

        /// <summary>
        ///   The option connection port to connect to control plane (Default : 5001)
        /// </summary>
        // TODO: mark as [PublicApi] for setter ?
        // ReSharper disable once MemberCanBePrivate.Global
        // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global
        public int ConnectionPort { get; set; } = 5001;

        /// <summary>
        ///   The TaskOptions to pass to the session or the submission session
        /// </summary>
        // TODO: mark as [PublicApi] for setter ?
        // ReSharper disable once MemberCanBePrivate.Global
        // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global
        public TaskOptions TaskOptions { get; set; }

        /// <summary>
        ///   The target name of the endpoint when ssl validation is disabled. Automatic if not set.
        /// </summary>
        // TODO: mark as [PublicApi] for setter ?
        // ReSharper disable once MemberCanBePrivate.Global
        // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global
        public string TargetNameOverride { get; set; } = "";

        /// <summary>
        ///   Initial backoff from retries
        /// </summary>
        public TimeSpan RetryInitialBackoff { get; } = TimeSpan.FromSeconds(1);

        /// <summary>
        ///   Backoff multiplier for retries
        /// </summary>
        public double RetryBackoffMultiplier { get; } = 2;

        /// <summary>
        ///   Max backoff for retries
        /// </summary>
        public TimeSpan RetryMaxBackoff { get; } = TimeSpan.FromSeconds(30);
    }

    public class MarkDownDocAttribute : Attribute
    {
    }
}