const url = "data.json";

fetch(url)
  .then(response => response.json())
  .then(data => {
    const postsContainer = document.getElementById("posts");

    data.forEach(postData => {
      const post = postData.post;
      const assets = postData.assets;

      const postElement = document.createElement("div");

      const textElement = document.createElement("p");
      textElement.textContent = post.full_text;
      postElement.appendChild(textElement);

      const postDetailsElement = document.createElement("ul");

      const idElement = document.createElement("li");
      idElement.textContent = `ID: ${post.id}`;
      postDetailsElement.appendChild(idElement);

      const favoriteCountElement = document.createElement("li");
      favoriteCountElement.textContent = `Favorites: ${post.favorite_count}`;
      postDetailsElement.appendChild(favoriteCountElement);

      const retweetCountElement = document.createElement("li");
      retweetCountElement.textContent = `Retweets: ${post.retweet_count}`;
      postDetailsElement.appendChild(retweetCountElement);

      const viewsElement = document.createElement("li");
      viewsElement.textContent = `Views: ${post.views}`;
      postDetailsElement.appendChild(viewsElement);

      const userIdElement = document.createElement("li");
      userIdElement.textContent = `User ID: ${post.user_id}`;
      postDetailsElement.appendChild(userIdElement);

      postElement.appendChild(postDetailsElement);

      assets.forEach(asset => {
        const imageElement = document.createElement("img");
        imageElement.src = asset.url;
        postElement.appendChild(imageElement);
      });

      postsContainer.appendChild(postElement);
    });
  });