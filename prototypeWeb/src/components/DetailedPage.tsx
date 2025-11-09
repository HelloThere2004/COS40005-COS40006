import React from 'react';

const errorsMock = [
  {
    id: 1,
    title: 'Lỗi 1: Xô S3',
    severity: 'Critical',
    suggestion: 'S3 bucket',
  },
  {
    id: 2,
    title: 'Lỗi 2: Xô S3',
    severity: 'Critical',
    suggestion: 'S3 bucket',
  },
  {
    id: 3,
    title: 'Lỗi 3: Xô S3',
    severity: 'Critical',
    suggestion: 'S3 bucket',
  },
];

const DetailedPage: React.FC = () => {
  return (
    <main className="flex-grow p-6">
      <div className="mx-auto max-w-6xl">
        {/* Outer panel with cyan background and thin black border to match the mockup */}
        <div className="rounded-md border-2 border-black bg-cyan-100 p-6">
          <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
            {/* Left: errors list (use two columns of 1/3 width left area) */}
            <div className="lg:col-span-2">
              <div className="space-y-6">
                {errorsMock.map((err, idx) => (
                  <div
                    key={err.id}
                    className={`rounded-md bg-white p-6 shadow-sm transition-all duration-150 ${
                      idx === 0 ? 'border-4 border-purple-400' : 'border border-gray-200'
                    }`}
                  >
                    <h3 className="text-center text-sm font-semibold text-gray-800">{err.title}</h3>
                    <p className="mt-3 text-center text-sm text-gray-600">Mức độ: {err.severity}</p>
                    <p className="mt-1 text-center text-sm text-gray-600">Gợi ý sửa: {err.suggestion}</p>
                  </div>
                ))}
              </div>
            </div>

            {/* Right: translated blog panel */}
            <div className="lg:col-span-1">
              <div className="h-full rounded-md bg-white p-6 shadow-lg">
                <h2 className="mb-4 text-center text-lg font-semibold text-gray-800">Translated blog</h2>
                <div className="h-[480px] overflow-auto rounded border border-gray-100 bg-white p-4 text-sm text-gray-700">
                  {/* Mock translated content - in the real app this should come from props/state */}
                  <p>
                    asdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
                    dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
                    dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
                  </p>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </main>
  );
};

export default DetailedPage;