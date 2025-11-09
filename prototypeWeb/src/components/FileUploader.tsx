import React from 'react';

const FileUploader: React.FC = () => {
  // Mock error data to demonstrate the UI

  return (
    <div className="w-full max-w-4xl">
      <div className="flex h-full flex-col rounded-lg bg-white p-6 shadow-lg">
        <h2 className="text-lg font-semibold text-gray-900">Processing Log</h2>
        <p className="mt-1 text-sm text-gray-500">
          Errors and warnings from the file processing will appear here automatically.
        </p>
        <div className="mt-4 flex-grow rounded-lg border-2 border-dashed border-gray-300 p-4">
          <textarea
            readOnly
            className="h-full min-h-[300px] w-full resize-none border-none bg-transparent p-2 font-mono text-sm text-red-600 focus:outline-none focus:ring-0"
            value="Please insert file "
          />
        </div>
      </div>
    </div>
  );
};

export default FileUploader;
