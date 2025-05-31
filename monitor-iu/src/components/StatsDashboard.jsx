import React, { useEffect, useState } from 'react';

const StatsDashboard = () => {
  const [summary, setSummary] = useState(null);
  const [costs, setCosts] = useState(null);

  useEffect(() => {
    fetch('http://localhost:5000/stats/summary')
      .then(res => res.json())
      .then(data => setSummary(data))
      .catch(err => console.error('Error fetching summary stats:', err));

    fetch('http://localhost:5000/stats/costs')
      .then(res => res.json())
      .then(data => setCosts(data))
      .catch(err => console.error('Error fetching cost stats:', err));
  }, []);

  if (!summary || !costs) return <p>Loading stats...</p>;

  return (
    <div style={{ padding: '2rem' }}>
      <h2>CDR Processing Dashboard</h2>
      <div style={{ display: 'flex', gap: '2rem', marginTop: '1rem' }}>
        <div>
          <h4>Summary</h4>
          <p>Total: {summary.total}</p>
          <p>Success: {summary.success}</p>
          <p>Failed: {summary.failed}</p>
        </div>
        <div>
          <h4>Costs</h4>
          <p>Average: €{costs.average_cost.toFixed(2)}</p>
          <p>Min: €{costs.min_cost.toFixed(2)}</p>
          <p>Max: €{costs.max_cost.toFixed(2)}</p>
        </div>
      </div>
    </div>
  );
};

export default StatsDashboard;