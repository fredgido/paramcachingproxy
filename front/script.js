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

            assets.forEach(asset => {
                const imageElement = document.createElement("img");
                imageElement.src = asset.url;
                postElement.appendChild(imageElement);
            });

            postsContainer.appendChild(postElement);
        });
    });