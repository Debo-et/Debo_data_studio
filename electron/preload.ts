const { contextBridge } = require('electron');

contextBridge.exposeInMainWorld('electronAPI', {
  // Add any APIs you need to expose to the renderer
  isElectron: true,
  platform: process.platform,
});