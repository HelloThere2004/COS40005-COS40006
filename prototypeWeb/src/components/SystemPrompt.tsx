import React from 'react';

const SystemPrompt: React.FC = () => {
  return (
    <div className="flex h-full flex-col rounded-lg bg-white p-6 shadow-lg">
      <h2 className="text-lg font-semibold text-gray-900">System Prompt</h2>
      <p className="mt-1 text-sm text-gray-500">
        Enter your instructions for the model below.
      </p>
      <div className="mt-4 flex-grow">
        <textarea
          className="h-full min-h-[200px] w-full resize-none rounded-md border border-gray-300 p-3 text-sm text-gray-800 shadow-sm focus:border-blue-500 focus:ring-blue-500"
          placeholder="e.g., Translate the following text to Vietnamese..."
        ></textarea>
      </div>
      <div className="mt-4 text-right">
        <button
          type="button"
          className="rounded-md bg-[#232F3E] px-6 py-2 font-medium text-white transition-colors duration-300 hover:bg-[#34495E]"
        >
          Generate
        </button>
      </div>
    </div>
  );
};

export default SystemPrompt;