import React from 'react';

const OutputComponent: React.FC = () => {
  // Mock data based on the user's sketch
  const outputData = {
    id: 'ART-001',
    errorTA: 'Minor grammar issues',
    errorTV: 'Incorrect translation',
    originalVersion: '1.0.2',
  };

  return (
    <div className="flex h-full flex-col rounded-lg bg-white p-6 shadow-lg">
      <h2 className="text-lg font-semibold text-gray-900">Output Component</h2>
      <p className="mt-1 text-sm text-gray-500">
        Displays detailed information and validation results.
      </p>
      <div className="mt-4 flex-grow rounded-lg border border-gray-200 bg-gray-50/50 p-4">
        <dl className="space-y-4">
          <div className="flex items-center justify-between">
            <dt className="text-sm font-medium text-gray-600">ID Bài</dt>
            <dd className="rounded-md bg-gray-200 px-2 py-1 text-sm font-semibold text-gray-800">
              {outputData.id}
            </dd>
          </div>
          <div className="flex items-center justify-between">
            <dt className="text-sm font-medium text-gray-600">Lỗi = TA</dt>
            <dd className="text-sm font-medium text-red-600">{outputData.errorTA}</dd>
          </div>
          <div className="flex items-center justify-between">
            <dt className="text-sm font-medium text-gray-600">Lỗi = TV</dt>
            <dd className="text-sm font-medium text-red-600">{outputData.errorTV}</dd>
          </div>
          <div className="flex items-center justify-between">
            <dt className="text-sm font-medium text-gray-600">Ver TA gốc</dt>
            <dd className="text-sm font-medium text-gray-800">{outputData.originalVersion}</dd>
          </div>
        </dl>
      </div>
    </div>
  );
};

export default OutputComponent;
