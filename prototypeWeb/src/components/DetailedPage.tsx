import React from 'react';
import SystemPrompt from './SystemPrompt';
import OutputComponent from './OutputComponent';

const DetailedPage: React.FC = () => {
  return (
    <main className="flex-grow p-4 sm:p-6 lg:p-8">
      <div className="container mx-auto">
        <div className="grid grid-cols-1 gap-8 lg:grid-cols-2">
          <div>
            <SystemPrompt />
          </div>
          <div>
            <OutputComponent />
          </div>
        </div>
      </div>
    </main>
  );
};

export default DetailedPage;