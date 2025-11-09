import React from 'react';
import { Article, ArticleStatus } from '../types';

interface ChartsPanelProps {
  articles: Article[];
}

const ChartCard: React.FC<{ title: string; children: React.ReactNode }> = ({ title, children }) => (
  <div className="mt-6 rounded-md border border-gray-200 p-4">
    <h3 className="text-base font-medium text-gray-700">{title}</h3>
    <div className="mt-4">{children}</div>
  </div>
);

const ChartsPanel: React.FC<ChartsPanelProps> = ({ articles }) => {
  // FIX: Explicitly type `statusCounts` to prevent type inference issues.
  const statusCounts: Record<ArticleStatus, number> = articles.reduce(
    (acc, article) => {
      acc[article.status] = (acc[article.status] || 0) + 1;
      return acc;
    },
    {} as Record<ArticleStatus, number>
  );

  const totalArticles = articles.length;

  const colors: Record<string, string> = {
    [ArticleStatus.PUBLISHED]: '#22c55e', // green-500
    [ArticleStatus.DRAFT]: '#eab308', // yellow-500
    [ArticleStatus.ARCHIVED]: '#9ca3af', // gray-400
  };

  const statusLabels: Record<ArticleStatus, string> = {
    [ArticleStatus.PUBLISHED]: 'Published',
    [ArticleStatus.DRAFT]: 'Draft',
    [ArticleStatus.ARCHIVED]: 'Archived',
  };

  let cumulativePercentage = 0;
  const gradientParts = Object.entries(statusCounts).map(([status, count]) => {
    // FIX: Prevent division by zero to avoid NaN, which can cause type issues in arithmetic operations.
    const percentage = totalArticles > 0 ? (count / totalArticles) * 100 : 0;
    const part = `${colors[status as ArticleStatus]} ${cumulativePercentage}% ${cumulativePercentage + percentage}%`;
    cumulativePercentage += percentage;
    return part;
  });

  const conicGradientStyle = { background: `conic-gradient(${gradientParts.join(', ')})` };

  return (
    <aside className="rounded-lg bg-white p-6 shadow-lg">
      <h2 className="text-lg font-semibold text-gray-900">Analytics Dashboard</h2>

      <ChartCard title="Status Distribution">
        <p className="text-sm text-gray-500">A breakdown of articles by their current status.</p>
        <div className="flex items-center justify-center py-6">
          <div
            className="h-36 w-36 rounded-full"
            style={conicGradientStyle}
            role="img"
            aria-label="Pie chart showing article status distribution"
          />
        </div>
        <div className="mt-4 space-y-2">
          {Object.entries(statusCounts).map(([status, count]) => (
            <div key={status} className="flex items-center justify-between text-sm">
              <div className="flex items-center">
                <span
                  className="mr-2 h-2.5 w-2.5 rounded-full"
                  style={{ backgroundColor: colors[status as ArticleStatus] }}
                />
                <span className="text-gray-600">{statusLabels[status as ArticleStatus]}</span>
              </div>
              <span className="font-medium text-gray-800">{count}</span>
            </div>
          ))}
        </div>
      </ChartCard>

      <ChartCard title="Content Overview">
        <div className="grid grid-cols-2 gap-4 text-center">
          <div>
            <p className="text-2xl font-bold text-[#232F3E]">{totalArticles}</p>
            <p className="text-xs text-gray-500">Total Articles</p>
          </div>
          <div>
            <p className="text-2xl font-bold text-[#232F3E]">
              {statusCounts[ArticleStatus.PUBLISHED] || 0}
            </p>
            <p className="text-xs text-gray-500">Published</p>
          </div>
        </div>
      </ChartCard>
    </aside>
  );
};

export default ChartsPanel;
