# Prisma Migrator

Migrate your data from one Prisma database to another seamlessly.

## Installation & Setup

1. **Clone the Repository**  
   Open your terminal and run:

   ```bash
   git clone https://github.com/its-anas/prisma-migrator
   ```

2. **Install Dependencies**  
   Navigate to the project directory and install the required packages:

   ```bash
   cd prisma-migrator
   npm install
   ```

3. **Add Your Prisma Folder**  
   Copy your Prisma folder to the root of this project.

4. **Generate Prisma Client**  
   Execute the following command to generate the Prisma client:

   ```bash
   npx prisma generate
   ```

5. **Configure Environment Variables**  
   Update the `.env` file with your source and destination database URLs.

6. **Run the Migrator**  
   Start the migration process by running:
   ```bash
   npm run migrate
   ```

## Important Note

Before running the migrator, ensure that your Prisma schema is deployed to the destination database. If not, use one of the following commands to apply the schema:

```bash
npx prisma migrate deploy
```

or

```bash
npx prisma db push
```

Happy migrating!
