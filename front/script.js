const submitButton = document.getElementById('submit-button');

submitButton.addEventListener('click', () => {
    const tweetIdInput = document.getElementById('tweet-id');
    const tweetId = tweetIdInput.value;
    const apiUrl = `https://tweet-api.fredgido.com/tweet?id=${tweetId}`;
    // Fetch the post data from the API endpoint
    fetch(apiUrl)
        .then(response => response.text())
        .then(rows => {

            console.log(rows);
            rows = JSONbig.parse(rows);
            for (row_index in rows) {
                row = rows[row_index]
                console.log(row);
                data = row.post
                assets = row.assets
                // Create a new Post object from the data
                const post = {
                    id: data.id,
                    full_text: data.full_text,
                    language: data.language,
                    retweet_count: data.retweet_count,
                    favorite_count: data.favorite_count,
                    reply_count: data.reply_count,
                    is_quote_status: data.is_quote_status,
                    views: data.views,
                    conversation_id: data.conversation_id,
                    hashtags: data.hashtags,
                    symbols: data.symbols,
                    user_mentions: data.user_mentions,
                    urls: data.urls,
                    is_retweet: data.is_retweet,
                    user_id: data.user_id,
                    processed_at: data.processed_at ? new Date(data.processed_at) : null,
                    images: assets.map(image => ({
                        id: image.id,
                        url: image.url
                    }))
                };

                // Create a new HTML element to display the post data
                const postElement = document.createElement('div');
                postElement.innerHTML = `
      <h1>Post ${post.id}</h1>
      <p>${post.full_text}</p>
      <ul>
        <li>Language: ${post.language}</li>
        <li>Retweets: ${post.retweet_count}</li>
        <li>Favorites: ${post.favorite_count}</li>
        <li>Replies: ${post.reply_count}</li>
        <li>Is Quote: ${post.is_quote_status}</li>
        <li>Views: ${post.views}</li>
        <li>Conversation ID: ${post.conversation_id}</li>
        <li>Hashtags: ${post.hashtags.join(', ')}</li>
        <li>Symbols: ${post.symbols.join(', ')}</li>
        <li>User Mentions: ${post.user_mentions.join(', ')}</li>
        <li>URLs: ${post.urls.join(', ')}</li>
        <li>Is Retweet: ${post.is_retweet}</li>
        <li>User ID: ${post.user_id}</li>
        <li>Processed At: ${post.processed_at ? post.processed_at.toLocaleString() : 'N/A'}</li>
      </ul>
    `;

                // Add the post images to the HTML element
                const imagesList = document.createElement('ul');
                imagesList.innerHTML = `
      <h2>Images:</h2>
      ${post.images.map(image => `<li><img src="${image.url}" alt="Image ${image.id}" height="600"></li>`).join('')}
    `;
                postElement.appendChild(imagesList);

                // Add the post element to the page
                const postContainer = document.getElementById('post-container');
                postContainer.appendChild(postElement);
            }
        })
        .catch(error => {
            console.error(error);
            alert('An error occurred while fetching the post data.');
        });
})



const recentButton = document.getElementById('recent-button');

recentButton.addEventListener('click', () => {
    const tweetIdInput = document.getElementById('tweet-id');
    const apiUrl = `https://tweet-api.fredgido.com/tweet?recent=true`;
    // Fetch the post data from the API endpoint
    fetch(apiUrl)
        .then(response => response.text())
        .then(rows => {

            console.log(rows);
            rows = JSONbig.parse(rows);
            for (row_index in rows) {
                row = rows[row_index]
                console.log(row);
                data = row.post
                assets = row.assets
                // Create a new Post object from the data
                const post = {
                    id: data.id,
                    full_text: data.full_text,
                    language: data.language,
                    retweet_count: data.retweet_count,
                    favorite_count: data.favorite_count,
                    reply_count: data.reply_count,
                    is_quote_status: data.is_quote_status,
                    views: data.views,
                    conversation_id: data.conversation_id,
                    hashtags: data.hashtags,
                    symbols: data.symbols,
                    user_mentions: data.user_mentions,
                    urls: data.urls,
                    is_retweet: data.is_retweet,
                    user_id: data.user_id,
                    processed_at: data.processed_at ? new Date(data.processed_at) : null,
                    images: assets.map(image => ({
                        id: image.id,
                        url: image.url
                    }))
                };

                // Create a new HTML element to display the post data
                const postElement = document.createElement('div');
                postElement.innerHTML = `
      <h1>Post ${post.id}</h1>
      <p>${post.full_text}</p>
      <ul>
        <li>Language: ${post.language}</li>
        <li>Retweets: ${post.retweet_count}</li>
        <li>Favorites: ${post.favorite_count}</li>
        <li>Replies: ${post.reply_count}</li>
        <li>Is Quote: ${post.is_quote_status}</li>
        <li>Views: ${post.views}</li>
        <li>Conversation ID: ${post.conversation_id}</li>
        <li>Hashtags: ${post.hashtags.join(', ')}</li>
        <li>Symbols: ${post.symbols.join(', ')}</li>
        <li>User Mentions: ${post.user_mentions.join(', ')}</li>
        <li>URLs: ${post.urls.join(', ')}</li>
        <li>Is Retweet: ${post.is_retweet}</li>
        <li>User ID: ${post.user_id}</li>
        <li>Processed At: ${post.processed_at ? post.processed_at.toLocaleString() : 'N/A'}</li>
      </ul>
    `;

                // Add the post images to the HTML element
                const imagesList = document.createElement('ul');
                imagesList.innerHTML = `
      <h2>Images:</h2>
      ${post.images.map(image => `<li><img src="${image.url}" alt="Image ${image.id}" height="600"></li>`).join('')}
    `;
                postElement.appendChild(imagesList);

                // Add the post element to the page
                const postContainer = document.getElementById('post-container');
                postContainer.appendChild(postElement);
            }
        })
        .catch(error => {
            console.error(error);
            alert('An error occurred while fetching the post data.');
        });
})