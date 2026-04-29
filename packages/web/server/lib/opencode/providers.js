import {
  CONFIG_FILE,
  readConfigLayers,
  isPlainObject,
  getConfigForPath,
  writeConfig,
} from './shared.js';

function getProviderSources(providerId, workingDirectory) {
  const layers = readConfigLayers(workingDirectory);
  const { userConfig, projectConfig, customConfig, paths } = layers;

  const customProviders = isPlainObject(customConfig?.provider) ? customConfig.provider : {};
  const customProvidersAlias = isPlainObject(customConfig?.providers) ? customConfig.providers : {};
  const projectProviders = isPlainObject(projectConfig?.provider) ? projectConfig.provider : {};
  const projectProvidersAlias = isPlainObject(projectConfig?.providers) ? projectConfig.providers : {};
  const userProviders = isPlainObject(userConfig?.provider) ? userConfig.provider : {};
  const userProvidersAlias = isPlainObject(userConfig?.providers) ? userConfig.providers : {};

  const customExists =
    Object.prototype.hasOwnProperty.call(customProviders, providerId) ||
    Object.prototype.hasOwnProperty.call(customProvidersAlias, providerId);
  const projectExists =
    Object.prototype.hasOwnProperty.call(projectProviders, providerId) ||
    Object.prototype.hasOwnProperty.call(projectProvidersAlias, providerId);
  const userExists =
    Object.prototype.hasOwnProperty.call(userProviders, providerId) ||
    Object.prototype.hasOwnProperty.call(userProvidersAlias, providerId);

  return {
    sources: {
      auth: { exists: false },
      user: { exists: userExists, path: paths.userPath },
      project: { exists: projectExists, path: paths.projectPath || null },
      custom: { exists: customExists, path: paths.customPath }
    }
  };
}

function removeProviderConfig(providerId, workingDirectory, scope = 'user') {
  if (!providerId || typeof providerId !== 'string') {
    throw new Error('Provider ID is required');
  }

  const layers = readConfigLayers(workingDirectory);
  let targetPath = layers.paths.userPath;

  if (scope === 'project') {
    if (!workingDirectory) {
      throw new Error('Working directory is required for project scope');
    }
    targetPath = layers.paths.projectPath || targetPath;
  } else if (scope === 'custom') {
    if (!layers.paths.customPath) {
      return false;
    }
    targetPath = layers.paths.customPath;
  }

  const targetConfig = getConfigForPath(layers, targetPath);
  const providerConfig = isPlainObject(targetConfig.provider) ? targetConfig.provider : {};
  const providersConfig = isPlainObject(targetConfig.providers) ? targetConfig.providers : {};
  const removedProvider = Object.prototype.hasOwnProperty.call(providerConfig, providerId);
  const removedProviders = Object.prototype.hasOwnProperty.call(providersConfig, providerId);

  if (!removedProvider && !removedProviders) {
    return false;
  }

  if (removedProvider) {
    delete providerConfig[providerId];
    if (Object.keys(providerConfig).length === 0) {
      delete targetConfig.provider;
    } else {
      targetConfig.provider = providerConfig;
    }
  }

  if (removedProviders) {
    delete providersConfig[providerId];
    if (Object.keys(providersConfig).length === 0) {
      delete targetConfig.providers;
    } else {
      targetConfig.providers = providersConfig;
    }
  }

  writeConfig(targetConfig, targetPath || CONFIG_FILE);
  console.log(`Removed provider ${providerId} from config: ${targetPath}`);
  return true;
}

function writeProviderConfig(providerId, providerConfigData, workingDirectory) {
  if (!providerId || typeof providerId !== 'string') {
    throw new Error('Provider ID is required');
  }
  if (!providerConfigData || typeof providerConfigData !== 'object') {
    throw new Error('Provider config data is required');
  }

  const layers = readConfigLayers(workingDirectory);
  const customProviders = isPlainObject(layers.customConfig?.provider) ? layers.customConfig.provider : {};
  const customProvidersAlias = isPlainObject(layers.customConfig?.providers) ? layers.customConfig.providers : {};
  const projectProviders = isPlainObject(layers.projectConfig?.provider) ? layers.projectConfig.provider : {};
  const projectProvidersAlias = isPlainObject(layers.projectConfig?.providers) ? layers.projectConfig.providers : {};
  const userProviders = isPlainObject(layers.userConfig?.provider) ? layers.userConfig.provider : {};
  const userProvidersAlias = isPlainObject(layers.userConfig?.providers) ? layers.userConfig.providers : {};

  let targetPath = layers.paths.userPath || CONFIG_FILE;
  let targetSection = 'provider';

  if (Object.prototype.hasOwnProperty.call(customProviders, providerId)) {
    targetPath = layers.paths.customPath || targetPath;
    targetSection = 'provider';
  } else if (Object.prototype.hasOwnProperty.call(customProvidersAlias, providerId)) {
    targetPath = layers.paths.customPath || targetPath;
    targetSection = 'providers';
  } else if (Object.prototype.hasOwnProperty.call(projectProviders, providerId)) {
    targetPath = layers.paths.projectPath || targetPath;
    targetSection = 'provider';
  } else if (Object.prototype.hasOwnProperty.call(projectProvidersAlias, providerId)) {
    targetPath = layers.paths.projectPath || targetPath;
    targetSection = 'providers';
  } else if (Object.prototype.hasOwnProperty.call(userProvidersAlias, providerId)) {
    targetSection = 'providers';
  } else if (Object.prototype.hasOwnProperty.call(userProviders, providerId)) {
    targetSection = 'provider';
  }

  const targetConfig = getConfigForPath(layers, targetPath);
  const targetProviders = isPlainObject(targetConfig[targetSection]) ? targetConfig[targetSection] : {};

  if (!targetConfig[targetSection] || !isPlainObject(targetConfig[targetSection])) {
    targetConfig[targetSection] = {};
  }

  // Merge with existing provider config to avoid overwriting other fields
  const existing = isPlainObject(targetProviders[providerId])
    ? { ...targetProviders[providerId] }
    : {};
  const hasIncomingOptions = Object.prototype.hasOwnProperty.call(providerConfigData, 'options');
  const incomingOptions = providerConfigData.options;
  const shouldClearAllOptions = hasIncomingOptions && incomingOptions === null;
  const existingOptions = isPlainObject(existing.options) ? { ...existing.options } : {};
  const newOptions = isPlainObject(incomingOptions) ? incomingOptions : {};

  const mergedOptions = shouldClearAllOptions
    ? {}
    : { ...existingOptions, ...newOptions };

  for (const [key, value] of Object.entries(newOptions)) {
    if (value === null) {
      delete mergedOptions[key];
    }
  }

  const nextProviderConfig = {
    ...existing,
    ...providerConfigData,
  };

  if (hasIncomingOptions) {
    if (Object.keys(mergedOptions).length > 0) {
      nextProviderConfig.options = mergedOptions;
    } else {
      delete nextProviderConfig.options;
    }
  }

  targetConfig[targetSection][providerId] = nextProviderConfig;

  writeConfig(targetConfig, targetPath);
  console.log(`Wrote provider ${providerId} to config: ${targetPath}`);
  return targetPath;
}

export {
  getProviderSources,
  removeProviderConfig,
  writeProviderConfig,
};
